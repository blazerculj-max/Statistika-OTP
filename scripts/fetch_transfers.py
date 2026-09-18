#!/usr/bin/env python3
"""
fetch_transfers.py — Prestopi med sezonama → data/transfers.json

KAJ PRIMERJA:
  Registracije lanske in letošnje sezone (NE odigranih tekem) — tako sta obe
  strani iz istega vira in primerljivi. Za vsako ekipo 1., 2. in 3. SKL:
    prišli  = letos prijavljeni, ki lani niso bili v članski ekipi ISTEGA kluba
    odšli   = lani prijavljeni v članski ekipi kluba, letos pa ne več

  Pri vsakem se pogleda, kje je bil lani (oz. kje je letos) kjerkoli v
  slovenski košarki — ne samo v teh treh ligah. Zato se pregledajo članska,
  U20 in U18 tekmovanja obeh sezon, vključno z ženskimi in pokali.

  Igralec iz mladinske ekipe istega kluba ni prestop, ampak napredovanje —
  označen je posebej ('own': true).

VIR:
  /teamPlayers/?teamId={id}&seasonId={N} — OBA parametra obvezna.
  (matchTeamId je tiho ignoriran in vrne cel register KZS — ne uporabljaj.)

UPORABA:
  python scripts/fetch_transfers.py                 # sezona iz seasons.json
  python scripts/fetch_transfers.py --season 27
  python scripts/fetch_transfers.py --refresh-prev  # znova zgradi lanski indeks

Lanski indeks se predpomni v data/_registrations_s{N}.json, ker se ne
spreminja — naslednji zagoni preberejo samo letošnjo sezono.
"""
import json, os, sys, time, urllib.request
from datetime import datetime, timezone

API_BASE = "https://api.kzs.si/api/v1/public"
DATA = "data"

LEAGUES = {
    'liga1': {'name': 'Liga OTP banka', 'gender': 'MALE',
              'category': 'ABSOLUTE', 'rank': 'FIRST',  'type': 'LEAGUE'},
    'liga2': {'name': '2. SKL',         'gender': 'MALE',
              'category': 'ABSOLUTE', 'rank': 'SECOND', 'type': 'LEAGUE'},
    'liga3': {'name': '3. SKL',         'gender': 'MALE',
              'category': 'ABSOLUTE', 'rank': 'THIRD',  'type': 'LEAGUE'},
}

# Kje vse iščemo prejšnjo/naslednjo postajo igralca.
SCAN_CATEGORIES = ('ABSOLUTE', 'U20', 'U18')
# Reprezentančna tekmovanja ("Pripravljalne tekme člani") niso klubi — igralec
# je tam registriran za reprezentanco, kar ni prestop. Brez tega bi se izpisalo
# npr. "v Slovenija · Pripravljalne tekme člani".
SKIP_TYPES = ('NATIONAL',)

REFRESH_PREV = '--refresh-prev' in sys.argv


def arg(name):
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
            return sys.argv[i + 1]
    return None


def fetch_json(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            print(f"    err ({i+1}): {e}", file=sys.stderr)
            time.sleep(i + 1)
    return None


def resolve_season():
    forced = arg('--season') or os.environ.get('SEASON_ID')
    if forced:
        return int(forced)
    try:
        with open(os.path.join(DATA, 'seasons.json'), encoding='utf-8') as f:
            cur = json.load(f).get('current')
        if cur is not None:
            return int(cur)
    except Exception:
        pass
    raise SystemExit("Ne najdem sezone (data/seasons.json → 'current'). Podaj --season N")


def competitions(sid):
    d = fetch_json(f"{API_BASE}/competitions/?seasonId={sid}")
    return (d or {}).get('data', {}).get('items', [])


def teams_of(comp_id, sid):
    d = fetch_json(f"{API_BASE}/teams/?competitionId={comp_id}&seasonId={sid}")
    return (d or {}).get('data', {}).get('items', [])


def roster(team_id, sid, limit=500):
    out, offset = [], 0
    for _ in range(10):
        d = fetch_json(f"{API_BASE}/teamPlayers/?teamId={team_id}"
                       f"&seasonId={sid}&limit={limit}&offset={offset}")
        items = (d or {}).get('data', {}).get('items', [])
        if not items:
            break
        out.extend(items)
        if len(items) < limit:
            break
        offset += limit
    return out


_CLUB_CACHE = {}


def club_of(team_id):
    """Klub ekipe — potreben, ker se team id vsako sezono spremeni."""
    if team_id in _CLUB_CACHE:
        return _CLUB_CACHE[team_id]
    d = fetch_json(f"{API_BASE}/teams/{team_id}")
    ta = ((d or {}).get('data') or {}).get('teamApplication') or {}
    cid = (ta.get('club') or {}).get('id')
    _CLUB_CACHE[team_id] = cid
    return cid


def build_index(sid, label):
    """
    Preleti vsa relevantna tekmovanja sezone in vrne:
      players: pid -> [{team, teamId, comp, category, rank}]
      teams:   teamId -> {name, comp, category, rank, players:set(pid)}
    """
    comps = [c for c in competitions(sid)
             if c.get('category') in SCAN_CATEGORIES and c.get('type') not in SKIP_TYPES]
    print(f"  {label}: {len(comps)} tekmovanj")
    seen_team = {}
    for c in comps:
        for t in teams_of(c['id'], sid):
            tid = t['id']
            if tid in seen_team:
                continue
            ta = t.get('teamApplication') or {}
            seen_team[tid] = {'name': ta.get('name') or f"ekipa {tid}",
                              'comp': c['name'], 'category': c.get('category'),
                              'rank': c.get('rank'), 'gender': c.get('gender')}
        time.sleep(0.02)
    print(f"  {label}: {len(seen_team)} ekip — berem sestave…")

    players, teams, people = {}, {}, {}
    for n, (tid, meta) in enumerate(sorted(seen_team.items()), 1):
        pids = set()
        for e in roster(tid, sid):
            p = e.get('player') or {}
            pid = p.get('id')
            if not pid:
                continue
            pids.add(pid)
            people.setdefault(pid, person(e))   # ime/letnik/slika tudi za odhode
            players.setdefault(pid, []).append({
                'team': meta['name'], 'teamId': tid, 'comp': meta['comp'],
                'category': meta['category'], 'rank': meta['rank'],
            })
        teams[tid] = {**meta, 'players': sorted(pids)}
        if n % 100 == 0:
            print(f"    {n}/{len(seen_team)}…")
        time.sleep(0.03)
    tot = sum(len(v['players']) for v in teams.values())
    print(f"  {label}: {len(players)} igralcev, {tot} registracij")
    return {'seasonId': sid, 'players': players, 'teams': teams, 'people': people}


def load_or_build_prev(prev_sid):
    path = os.path.join(DATA, f"_registrations_s{prev_sid}.json")
    if os.path.exists(path) and not REFRESH_PREV:
        try:
            with open(path, encoding='utf-8') as f:
                idx = json.load(f)
            idx['players'] = {int(k): v for k, v in idx['players'].items()}
            idx['teams'] = {int(k): v for k, v in idx['teams'].items()}
            idx['people'] = {int(k): v for k, v in idx.get('people', {}).items()}
            print(f"  lanski indeks iz predpomnilnika ({len(idx['players'])} igralcev)")
            return idx
        except Exception as e:
            print(f"  ! predpomnilnik ni berljiv ({e}) — gradim znova")
    idx = build_index(prev_sid, f"sezona {prev_sid}")
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(idx, f, ensure_ascii=False, separators=(',', ':'))
    print(f"  ✓ {path}")
    return idx


def person(entry):
    p = entry.get('player') or {}
    per = p.get('person') or {}
    return {
        'pid': p.get('id'),
        'name': f"{(per.get('firstName') or '').strip()} {(per.get('lastName') or '').strip()}".strip(),
        'last': (per.get('lastName') or '').strip(),
        'born': per.get('yearOfBirth'),
        'photo': p.get('photoUuid'),
        'nat': (p.get('nationality') or {}).get('alpha2Code'),
    }


def pick_station(stations, exclude_team_ids=()):
    """Izmed lanskih/letošnjih postaj izberi najpomembnejšo (najvišji rang, člani pred mladinci)."""
    order = {'ABSOLUTE': 0, 'U20': 1, 'U18': 2}
    rank = {'FIRST': 0, 'SECOND': 1, 'THIRD': 2, 'FOURTH': 3}
    cand = [s for s in stations if s['teamId'] not in exclude_team_ids]
    if not cand:
        return None
    cand.sort(key=lambda s: (order.get(s.get('category'), 9), rank.get(s.get('rank'), 9)))
    return cand[0]


def main():
    if not os.path.isdir(DATA):
        raise SystemExit(f"Mapa {DATA}/ ne obstaja — poženi iz korena repozitorija.")
    sid = resolve_season()
    prev = sid - 1
    print(f"=== Prestopi {prev} → {sid} ===")

    print("\nLanska sezona:")
    pidx = load_or_build_prev(prev)
    print("\nLetošnja sezona:")
    cidx = build_index(sid, f"sezona {sid}")

    # klub → članska ekipa lani / letos
    print("\nPovezujem klube čez sezoni…")
    prev_senior_club = {}
    for tid, m in pidx['teams'].items():
        if m.get('category') == 'ABSOLUTE' and m.get('gender') == 'MALE':
            cid = club_of(tid)
            if cid and cid not in prev_senior_club:
                prev_senior_club[cid] = tid
            time.sleep(0.02)
    print(f"  lanskih članskih ekip s klubom: {len(prev_senior_club)}")

    out_leagues = {}
    season_name = None
    n_in = n_out = 0
    for key, spec in LEAGUES.items():
        c = next((x for x in competitions(sid)
                  if x.get('gender') == spec['gender'] and x.get('category') == spec['category']
                  and x.get('rank') == spec['rank'] and x.get('type') == spec['type']), None)
        if not c:
            print(f"  ! {key}: tekmovanja ni")
            continue
        season_name = season_name or (c.get('season') or {}).get('name')
        print(f"\n--- {c['name']} ---")
        teams_out = []
        for t in teams_of(c['id'], sid):
            tid = t['id']
            ta = t.get('teamApplication') or {}
            name = ta.get('name') or f"ekipa {tid}"
            cid = club_of(tid)
            entries = roster(tid, sid)
            now = {}
            for e in entries:
                pr = person(e)
                if pr['pid']:
                    now[pr['pid']] = pr
            prev_tid = prev_senior_club.get(cid)
            prev_squad = set(pidx['teams'].get(prev_tid, {}).get('players', [])) if prev_tid else set()

            arrivals = []
            for pid, pr in now.items():
                if pid in prev_squad:
                    continue
                st = pick_station(pidx['players'].get(pid, []))
                # Enako merilo kot pri odhodih: ista KLUBSKA pripadnost (ne le
                # članska ekipa), da je napredovanje iz lastne mladinske ekipe
                # označeno kot notranji premik in ne kot prestop.
                own = bool(st and cid and club_of(st['teamId']) == cid)
                arrivals.append({**pr, 'from': ({'team': st['team'], 'comp': st['comp']} if st else None),
                                 'own': own})
            departures = []
            for pid in prev_squad:
                if pid in now:
                    continue
                st = pick_station(cidx['players'].get(pid, []), exclude_team_ids={tid})
                pr = pidx['people'].get(pid) or {'pid': pid, 'name': f'igralec {pid}', 'last': '',
                                                 'born': None, 'photo': None, 'nat': None}
                # Premik v mladinsko ekipo ISTEGA kluba ni odhod iz kluba.
                own_out = bool(st and cid and club_of(st['teamId']) == cid)
                departures.append({**pr, 'own': own_out,
                                   'to': ({'team': st['team'], 'comp': st['comp']} if st else None)})
            arrivals.sort(key=lambda p: (p['last'].lower(), p['name'].lower()))
            departures.sort(key=lambda p: ((p.get('last') or '').lower(), p['name'].lower()))
            n_in += len(arrivals); n_out += len(departures)
            print(f"   {name:26s} prišli {len(arrivals):2d} · odšli {len(departures):2d}")
            teams_out.append({'id': tid, 'name': name, 'logo': t.get('logoUuid'),
                              'prevTeamId': prev_tid,
                              'squadNow': len(now), 'squadPrev': len(prev_squad),
                              'in': arrivals, 'out': departures})
            time.sleep(0.03)
        out_leagues[key] = {'name': c['name'], 'teams': teams_out}

    payload = {
        'generated': datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
        'seasonId': sid, 'prevSeasonId': prev,
        'season': season_name or str(sid),
        'leagues': out_leagues,
    }
    path = os.path.join(DATA, 'transfers.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
    print(f"\nSKUPAJ prišlo {n_in} · odšlo {n_out}")
    print(f"✅ {path} ({os.path.getsize(path)//1024} KB)")


if __name__ == "__main__":
    main()
