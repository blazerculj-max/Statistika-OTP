#!/usr/bin/env python3
"""
fetch_rosters.py — Prijavljeni igralci po ekipah (1., 2. in 3. SKL) → data/rosters.json

ZAKAJ POSEBEJ:
  Statistika nastane šele iz odigranih tekem. Sestavi pa obstajajo že prej —
  klubi igralce prijavijo pred začetkom sezone. To je edini vir imen in
  fotografij v predsezonskem obdobju.

KLJUČNO O API-ju:
  Sestav vrne /teamPlayers/?teamId={id}&seasonId={N} — OBA parametra sta
  potrebna. Sam teamId vrne prazen seznam, matchTeamId pa je tiho ignoriran
  in vrne abecedni izsek CELOTNEGA registra KZS (vseh starosti in obeh
  spolov) — torej napačne podatke brez napake. Ne uporabljaj ju.
  Zapis igralca že vsebuje photoUuid, zato dodatnih klicev za slike ni.

UPORABA:
  python scripts/fetch_rosters.py              # sezona iz data/seasons.json
  python scripts/fetch_rosters.py --season 27
  python scripts/fetch_rosters.py --probe      # samo izpiši, ne zapiši

Po tem poženi scripts/download_images.py, da prenese nove fotografije.
"""
import json, os, sys, time, urllib.request
from datetime import datetime, timezone

API_BASE = "https://api.kzs.si/api/v1/public"
DATA = "data"

# Ista četverica kot v fetch_data.py — stabilna čez sezone (ime je sponzorsko).
LEAGUES = {
    'liga1': {'name': 'Liga OTP banka', 'gender': 'MALE',
              'category': 'ABSOLUTE', 'rank': 'FIRST',  'type': 'LEAGUE'},
    'liga2': {'name': '2. SKL',         'gender': 'MALE',
              'category': 'ABSOLUTE', 'rank': 'SECOND', 'type': 'LEAGUE'},
    'liga3': {'name': '3. SKL',         'gender': 'MALE',
              'category': 'ABSOLUTE', 'rank': 'THIRD',  'type': 'LEAGUE'},
}

PROBE = '--probe' in sys.argv


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
    """--season N / SEASON_ID / 'current' iz seasons.json (vzdržuje ga fetch_data)."""
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


def find_competition(season_id, spec):
    d = fetch_json(f"{API_BASE}/competitions/?seasonId={season_id}")
    for c in (d or {}).get('data', {}).get('items', []):
        if (c.get('gender') == spec['gender'] and c.get('category') == spec['category']
                and c.get('rank') == spec['rank'] and c.get('type') == spec['type']):
            return c
    return None


def fetch_teams(comp_id, season_id):
    d = fetch_json(f"{API_BASE}/teams/?competitionId={comp_id}&seasonId={season_id}")
    return (d or {}).get('data', {}).get('items', [])


def fetch_roster(team_id, season_id, limit=500):
    """Prijavljeni igralci ekipe. OBA parametra sta obvezna — glej docstring."""
    out, offset = [], 0
    for _ in range(10):
        d = fetch_json(f"{API_BASE}/teamPlayers/?teamId={team_id}"
                       f"&seasonId={season_id}&limit={limit}&offset={offset}")
        items = (d or {}).get('data', {}).get('items', [])
        if not items:
            break
        out.extend(items)
        if len(items) < limit:
            break
        offset += limit
    return out


def slim(entry):
    p = entry.get('player') or {}
    per = p.get('person') or {}
    nat = p.get('nationality') or {}
    name = f"{(per.get('firstName') or '').strip()} {(per.get('lastName') or '').strip()}".strip()
    return {
        'pid': p.get('id'),
        'name': name,
        'last': (per.get('lastName') or '').strip(),   # za razvrščanje po priimku
        'born': per.get('yearOfBirth'),
        'nat': nat.get('name'),
        'natCode': nat.get('alpha2Code'),
        'homegrown': bool(p.get('homegrown')),
        'photo': p.get('photoUuid'),
        'positions': [x.get('name') if isinstance(x, dict) else x
                      for x in (p.get('positions') or [])],
    }


def main():
    if not os.path.isdir(DATA):
        raise SystemExit(f"Mapa {DATA}/ ne obstaja — poženi iz korena repozitorija.")
    sid = resolve_season()
    print(f"=== Sestavi · sezona {sid}{' [PROBE]' if PROBE else ''} ===")

    season_name = None
    leagues, grand, with_photo = {}, 0, 0

    for key, spec in LEAGUES.items():
        c = find_competition(sid, spec)
        if not c:
            print(f"  ! {key}: tekmovanja ni v sezoni {sid} — preskačem")
            continue
        season_name = season_name or (c.get('season') or {}).get('name')
        print(f"\n--- {c['name']} (comp {c['id']}) ---")
        teams_out, n_lg = [], 0
        for t in fetch_teams(c['id'], sid):
            ta = t.get('teamApplication') or {}
            nm = ta.get('name') or f"ekipa {t['id']}"
            entries = fetch_roster(t['id'], sid)
            players = [slim(e) for e in entries if e.get('player')]
            # en igralec je lahko prijavljen večkrat (npr. A in B ekipa)
            seen, uniq = set(), []
            for p in players:
                if p['pid'] in seen:
                    continue
                seen.add(p['pid']); uniq.append(p)
            uniq.sort(key=lambda p: (p['last'].lower(), p['name'].lower()))
            n_ph = sum(1 for p in uniq if p['photo'])
            flag = '' if uniq else '   ← še ni prijav'
            print(f"   {nm:26s} {len(uniq):3d} igralcev · {n_ph:3d} s sliko{flag}")
            teams_out.append({
                'id': t['id'], 'name': nm, 'code': ta.get('code'),
                'logo': t.get('logoUuid'), 'players': uniq,
            })
            n_lg += len(uniq); with_photo += n_ph
            time.sleep(0.05)
        leagues[key] = {'name': c['name'], 'competitionId': c['id'], 'teams': teams_out}
        grand += n_lg

    if not leagues:
        raise SystemExit("Nobene lige — ne zapišem ničesar.")

    payload = {
        'generated': datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
        'seasonId': sid,
        'season': season_name or str(sid),
        'leagues': leagues,
    }

    path = os.path.join(DATA, 'rosters.json')
    # Varovalka: praznega nabora ne zapišemo čez obstoječega (napaka API-ja).
    if grand == 0 and os.path.exists(path):
        try:
            with open(path, encoding='utf-8') as f:
                prev = json.load(f)
            had = sum(len(t['players']) for lg in prev.get('leagues', {}).values()
                      for t in lg.get('teams', []))
            if had:
                print(f"\n⚠ API ni vrnil nobenega igralca, obstoječih {had} obdržim — "
                      f"{path} nedotaknjen")
                return
        except Exception:
            pass

    print(f"\nSKUPAJ: {grand} prijavljenih igralcev · {with_photo} s fotografijo")
    if PROBE:
        print(f"» {path} [probe — ne zapišem]")
        return
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
    print(f"✅ {path} ({os.path.getsize(path)//1024} KB)")
    print("\nZdaj poženi: python scripts/download_images.py")


if __name__ == "__main__":
    main()
