#!/usr/bin/env python3
"""
fetch_mladi.py — Slovenske mladinske lige (KZS) → data/mladi_u{XX}.json + manifest.

Rekonstruira strukturo, ki jo bere zavihek 🌱 Mladi:
  mladi_manifest.json   {generated, season, ages:{u18:{label,ranks,file}, …}}
  mladi_u18.json        {generated, season, seasonId, age, label,
                         ranks:{ "1.A": {label, competitionId, teams:{}, regular:[], finals:[]}, … }}

Igralec: {pid,name,team,teamId,born,photo,gp,ppg,ptsTotal,ptsMax,
          trend:[{r,d,pts,opp,home,ts,os,w}], hasBox, box:{…}}

  - "regular" = skupinske faze (redni del), "finals" = izločilne/zaključni turnir.
  - V rednem delu KZS praviloma beleži SAMO točke; poln boxscore je na
    zaključnem turnirju → hasBox pove, ali so box podatki verodostojni.

ID-ji tekmovanj se odkrijejo iz API-ja (kategorija + rang + tip), zato ob novi
sezoni tu ni ničesar za posodabljati.

UPORABA:
  python scripts/fetch_mladi.py                    # vse starosti, trenutna sezona
  python scripts/fetch_mladi.py --ages u16         # samo U16
  python scripts/fetch_mladi.py --ranks 1.A        # samo 1.A SKL (hitro)
  python scripts/fetch_mladi.py --season 27
  python scripts/fetch_mladi.py --probe            # samo pokaži, kaj bi pobral
"""
import json, os, sys, time, urllib.request
from datetime import datetime, timezone

API_BASE = "https://api.kzs.si/api/v1/public"
DATA = "data"

AGES = ['u18', 'u16', 'u14']          # privzete starosti (moški)
# ključ ranga → (rank, type) v /competitions
RANKS = {
    '1.A': ('FIRST',  'LEAGUE_A'),
    '1.B': ('FIRST',  'LEAGUE_B'),
    '2.':  ('SECOND', 'LEAGUE'),
}
# Faza šteje za "finals", če je izločilna (BRACKET_*) ali se tako imenuje.
FINALS_HINTS = ('turnir', 'izločiln', 'izlocil', 'finale', 'finala')


def arg(name, default=None):
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
            return sys.argv[i + 1]
    return default


PROBE = '--probe' in sys.argv
SEL_AGES = [a.strip() for a in (arg('--ages') or ','.join(AGES)).split(',') if a.strip()]
SEL_RANKS = [r.strip() for r in (arg('--ranks') or ','.join(RANKS)).split(',') if r.strip()]


def fetch_json(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            print(f"    err ({i+1}): {e}", file=sys.stderr)
            time.sleep(i + 1)
    return None


def resolve_season():
    """--season N / SEASON_ID / najnovejša sezona z objavljenimi mladinskimi ligami."""
    forced = arg('--season') or os.environ.get('SEASON_ID')
    if forced:
        return int(forced)
    d = fetch_json(f"{API_BASE}/seasons/")
    items = (d or {}).get('data', {}).get('items', [])

    def start_year(s):
        try:
            return int(str(s.get('name', ''))[:4])
        except ValueError:
            return 0
    for s in sorted(items, key=start_year, reverse=True)[:6]:
        comps = competitions(s['id'])
        if find_comp(comps, SEL_AGES[0].upper(), *RANKS['1.A']):
            return s['id']
    raise SystemExit("Ne najdem sezone z mladinskimi ligami — podaj --season N")


def competitions(season_id):
    d = fetch_json(f"{API_BASE}/competitions/?seasonId={season_id}")
    return (d or {}).get('data', {}).get('items', [])


def find_comp(comps, category, rank, ctype):
    for c in comps:
        if (c.get('gender') == 'MALE' and c.get('category') == category
                and c.get('rank') == rank and c.get('type') == ctype):
            return c
    return None


def phases(comp_id):
    d = fetch_json(f"{API_BASE}/competitions/{comp_id}")
    out = []
    for p in (d or {}).get('data', {}).get('phases', []):
        name = (p.get('name') or '')
        is_finals = str(p.get('type', '')).startswith('BRACKET') or \
                    any(h in name.lower() for h in FINALS_HINTS)
        out.append({'id': p['id'], 'name': name, 'finals': is_finals,
                    'groups': [g['id'] for g in p.get('groups', []) if g.get('id')]})
    return out


# POZOR: KZS API ignorira &page (vsaka stran vrne isti sveženj) — delujoč
# parameter je &offset. Prav tako je &competitionPhaseGroupId pri večjih
# mladinskih tekmovanjih tiho ignoriran, zato pobiramo po FAZAH.
def fetch_matches(comp_id, season_id, phase_id=None, limit=1000):
    out, offset = [], 0
    for _ in range(50):
        u = (f"{API_BASE}/matches/?competitionId={comp_id}&seasonId={season_id}"
             f"&limit={limit}&offset={offset}")
        if phase_id: u += f"&competitionPhaseId={phase_id}"
        d = fetch_json(u)
        items = (d or {}).get('data', {}).get('items', [])
        if not items: break
        out.extend(items)
        if len(items) < limit: break
        offset += limit
        time.sleep(0.1)
    return out


def has_full_box(match_stats):
    """Ali tekma sploh ima poln boxscore (ne le točk)?"""
    for side in ('firstTeam', 'secondTeam'):
        for ps in (match_stats.get(side) or {}).get('playerStats', []) or []:
            if ps.get('minutes') or ps.get('totalRebounds') or ps.get('assists'):
                return True
    return False


def blank_player(ps, team_name, team_id):
    return {
        'pid': ps.get('playerId'),
        'name': (f"{(ps.get('matchTeamPlayerFirstname') or '').strip()} "
                 f"{(ps.get('matchTeamPlayerLastname') or '').strip()}").strip(),
        'team': team_name, 'teamId': team_id,
        'born': str(ps.get('matchTeamPlayerYearOfBirth') or ''),
        'photo': ps.get('photoUuid'),
        'gp': 0, 'ppg': 0, 'ptsTotal': 0, 'ptsMax': 0,
        'trend': [], 'hasBox': False,
        '_box': {k: 0 for k in ('g','min','reb','ast','stl','blk','tov','pir',
                                'fg2m','fg2a','fg3m','fg3a','ftm','fta')},
    }


def build_segment(matches, season_id, label):
    """Iz tekem zgradi seznam igralcev s trendom in (če obstaja) boxscorom."""
    finished = [m for m in matches if m.get('status') == 'FINISHED']
    players = {}
    for i, m in enumerate(finished):
        d = fetch_json(f"{API_BASE}/matches/{m['id']}/stats")
        md = (d or {}).get('data')
        if not md:
            continue
        full_box = has_full_box(md)
        sides = [('firstTeam',  m.get('firstTeamName'),  m.get('firstTeamTeamId'),
                  m.get('firstTeamScore'),  m.get('secondTeamScore'), m.get('secondTeamName'), True),
                 ('secondTeam', m.get('secondTeamName'), m.get('secondTeamTeamId'),
                  m.get('secondTeamScore'), m.get('firstTeamScore'),  m.get('firstTeamName'), False)]
        for key, tname, tid, ts, os_, opp, home in sides:
            td = md.get(key) or {}
            for ps in td.get('playerStats') or []:
                pts = ps.get('points', 0) or 0
                if not ps.get('played') and not pts:
                    continue
                pid = ps.get('playerId')
                if pid is None:
                    continue
                p = players.setdefault(pid, blank_player(ps, tname, tid))
                p['team'], p['teamId'] = tname, tid   # zadnja ekipa (ob prestopu)
                if ps.get('photoUuid'):
                    p['photo'] = ps['photoUuid']
                p['gp'] += 1
                p['ptsTotal'] += pts
                p['ptsMax'] = max(p['ptsMax'], pts)
                p['trend'].append({
                    'r': m.get('round', 0),
                    'd': (m.get('dateTime') or '')[:10],
                    'pts': pts, 'opp': opp, 'home': home,
                    'ts': ts, 'os': os_,
                    'w': (ts is not None and os_ is not None and ts > os_),
                })
                if full_box:
                    b = p['_box']
                    b['g']   += 1
                    b['min'] += ps.get('minutes', 0) or 0
                    b['reb'] += ps.get('totalRebounds', 0) or 0
                    b['ast'] += ps.get('assists', 0) or 0
                    b['stl'] += ps.get('steals', 0) or 0
                    b['blk'] += ps.get('blocksInFavor', 0) or 0
                    b['tov'] += ps.get('turnovers', 0) or 0
                    b['pir'] += ps.get('pIR', 0) or 0
                    b['fg2m'] += ps.get('twoPM', 0) or 0;   b['fg2a'] += ps.get('twoPA', 0) or 0
                    b['fg3m'] += ps.get('threePM', 0) or 0; b['fg3a'] += ps.get('threePA', 0) or 0
                    b['ftm']  += ps.get('fTM', 0) or 0;     b['fta']  += ps.get('fTA', 0) or 0
                    p['hasBox'] = True
        if (i + 1) % 25 == 0:
            print(f"    {label}: {i+1}/{len(finished)} tekem")
        time.sleep(0.05)

    out = []
    for p in players.values():
        p['trend'].sort(key=lambda t: (t['r'], t['d']))
        g = max(p['gp'], 1)
        p['ppg'] = round(p['ptsTotal'] / g, 1)
        b = p.pop('_box')
        if p['hasBox'] and b['g']:
            n = b['g']
            p['box'] = {
                'g': n, 'mpg': round(b['min']/n, 1), 'rpg': round(b['reb']/n, 1),
                'apg': round(b['ast']/n, 1), 'spg': round(b['stl']/n, 1),
                'bpg': round(b['blk']/n, 1), 'topg': round(b['tov']/n, 1),
                'pir': round(b['pir']/n, 1),
                'fg2m': b['fg2m'], 'fg2a': b['fg2a'], 'fg3m': b['fg3m'],
                'fg3a': b['fg3a'], 'ftm': b['ftm'], 'fta': b['fta'],
            }
        out.append(p)
    out.sort(key=lambda p: -p['ptsTotal'])
    return out


def teams_map(matches):
    t = {}
    for m in matches:
        for tid, name, logo, code in (
                (m.get('firstTeamTeamId'),  m.get('firstTeamName'),
                 m.get('firstTeamLogoUuid'),  m.get('firstTeamCode')),
                (m.get('secondTeamTeamId'), m.get('secondTeamName'),
                 m.get('secondTeamLogoUuid'), m.get('secondTeamCode'))):
            if tid and name:
                t[str(tid)] = {'name': name, 'logo': logo, 'code': code or ''}
    return t


def process_rank(comps, age, rank_key, season_id):
    rank, ctype = RANKS[rank_key]
    c = find_comp(comps, age.upper(), rank, ctype)
    if not c:
        print(f"  – {age} {rank_key}: tekmovanja ni v sezoni {season_id}")
        return None
    ph = phases(c['id'])
    print(f"  {age} {rank_key}: {c['name']} (comp {c['id']}) · "
          f"faze {[(p['name'], 'F' if p['finals'] else 'R') for p in ph]}")

    reg_matches, fin_matches = [], []
    for p in ph:
        ms = fetch_matches(c['id'], season_id, p['id'])
        (fin_matches if p['finals'] else reg_matches).extend(ms)

    def dedup(ms):
        seen = set()
        return [m for m in ms if not (m['id'] in seen or seen.add(m['id']))]
    reg_matches, fin_matches = dedup(reg_matches), dedup(fin_matches)
    print(f"    tekem: redni del {len(reg_matches)}, končnica {len(fin_matches)}")

    if PROBE:
        return {'label': c['name'], 'competitionId': c['id'],
                'teams': teams_map(reg_matches + fin_matches),
                'regular': [], 'finals': [],
                '_probe': {'regular': len(reg_matches), 'finals': len(fin_matches)}}

    return {
        'label': c['name'],
        'competitionId': c['id'],
        'teams': teams_map(reg_matches + fin_matches),
        'regular': build_segment(reg_matches, season_id, f"{age} {rank_key} redni"),
        'finals':  build_segment(fin_matches, season_id, f"{age} {rank_key} končnica"),
    }


def main():
    os.makedirs(DATA, exist_ok=True)
    sid = resolve_season()
    comps = competitions(sid)
    sname = next((c['season']['name'] for c in comps if c.get('season')), str(sid))
    now = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    print(f"=== Mladi (SI) · sezona {sid} ({sname}) "
          f"· starosti {SEL_AGES} · rangi {SEL_RANKS}{' [PROBE]' if PROBE else ''} ===")

    manifest_ages = {}
    for age in SEL_AGES:
        print(f"\n--- {age.upper()} ---")
        ranks = {}
        for rk in SEL_RANKS:
            if rk not in RANKS:
                print(f"  ! neznan rang {rk} — preskačem")
                continue
            r = process_rank(comps, age, rk, sid)
            if r:
                ranks[rk] = r
        if not ranks:
            continue
        payload = {'generated': now, 'season': sname, 'seasonId': sid,
                   'age': age, 'label': age.upper(), 'ranks': ranks}
        fname = f"mladi_{age}.json"
        if PROBE:
            print(f"  » {fname}: " + ', '.join(
                f"{k}={v['_probe']}" for k, v in ranks.items()))
        else:
            with open(os.path.join(DATA, fname), 'w', encoding='utf-8') as f:
                json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
            print(f"  ✅ {fname} ({os.path.getsize(os.path.join(DATA, fname))//1024} KB)")
        manifest_ages[age] = {'label': age.upper(), 'ranks': list(ranks), 'file': fname}

    if manifest_ages and not PROBE:
        # Ohrani starosti, ki jih ta zagon ni osvežil (npr. pri --ages u16).
        mpath = os.path.join(DATA, 'mladi_manifest.json')
        prev = {}
        if os.path.exists(mpath):
            try:
                with open(mpath, encoding='utf-8') as f:
                    prev = json.load(f).get('ages', {})
            except Exception:
                pass
        prev.update(manifest_ages)
        with open(mpath, 'w', encoding='utf-8') as f:
            json.dump({'generated': now, 'season': sname, 'ages': prev},
                      f, ensure_ascii=False, indent=2)
        print(f"\n✅ mladi_manifest.json ({list(prev)})")
    print("\n=== Done! ===")


if __name__ == "__main__":
    main()
