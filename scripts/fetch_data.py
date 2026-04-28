#!/usr/bin/env python3
"""
KZS Data Fetcher — Incremental, brez PBP (PBP je ločen job).
"""

import json, time, urllib.request, os, sys
from datetime import datetime, timezone, timedelta

API_BASE  = "https://api.kzs.si/api/v1/public"
FIBA_BASE = "https://fibalivestats.dcd.shared.geniussports.com/data"
SEASON_ID = 26

# Liga1: phase ID za moško 1. SKL — izključuje žensko ligo
# Preveriti: fetch brez filtra vrača tudi ženske tekme!
LEAGUES = {
    'liga1': {
        'id': 579,
        'name': 'Liga OTP banka',
        'phase_ids': None,   # single page fetch, samo 1 stran (135 tekem)
        'groups': {},
        'max_pages': 1,      # ← samo prva stran, ne paginira
    },
    'liga2': {
        'id': 581,
        'name': '2. SKL',
        'phase_ids': [5813, 5873, 5874, 5880, 5885],
        'groups': {},
        'max_pages': 99,
    },
    'liga3': {
        'id': 582,
        'name': '3. SKL',
        # Redni del + celoten playoff (phase IDs potrjeni 28.4.2026)
        # 5814=Redni del, 5868=Osmina finala, 5869=Liga za obstanek
        # 5878=Četrtfinale, 5884=Polfinale
        'phase_ids': [5814, 5868, 5869, 5878, 5884],
        'max_pages': 2,
        'known_teams': {'Konjice','Branik Maribor','Bistrica Kety Emmi','Innoduler Dravograd Koroška',
                        'Vojnik G7','Elektra Šoštanj','Hrastnik','Vrani Vransko','Kovinarstvo Bučar Miklavž','Nazarje',
                        'Leone Ajdovščina','Armicafe Troti','Cedevita Olimpija mladi','Koper',
                        'Mesarija Prunk Sežana','Kolpa','Litija','Janče ECP Tactical','Gorenja vas','Tera Tolmin'},
        'groups': {},
    },
}

FORCE_FULL  = '--full'  in sys.argv
FETCH_PBP   = '--pbp'   in sys.argv  # PBP samo če eksplicitno zahtevano
STATS_ONLY  = '--stats' in sys.argv

def fetch_json(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            print(f"  Err ({i+1}): {e}")
            time.sleep(i + 1)
    return None

def fetch_phase(comp_id, phase_id=None, group_id=None, max_pages=99, known_teams=None):
    all_items = []
    page = 1
    while page <= max_pages:
        url = f"{API_BASE}/matches/?competitionId={comp_id}&seasonId={SEASON_ID}"
        if phase_id: url += f"&competitionPhaseId={phase_id}"
        if group_id: url += f"&competitionPhaseGroupId={group_id}"
        if page > 1:  url += f"&page={page}"
        data = fetch_json(url)
        items = data.get('data', {}).get('items', []) if data else []
        if not items: break
        # If known_teams provided, stop if no known teams on this page
        if known_teams:
            page_known = [m for m in items
                         if m['firstTeamName'] in known_teams and m['secondTeamName'] in known_teams]
            if not page_known and page > 1:
                print(f"    Stran {page}: ni znanih ekip → ustavljam")
                break
            all_items.extend(items)
        else:
            all_items.extend(items)
        if len(items) < 150: break
        page += 1
        time.sleep(0.1)
    return all_items

def fetch_all_matches(key, lg):
    max_p = lg.get('max_pages', 99)
    # Known teams for liga2/liga3 to stop pagination early
    known = lg.get('known_teams')
    if lg['phase_ids']:
        all_items = []
        for pid in lg['phase_ids']:
            gids = lg['groups'].get(pid)
            if gids:
                for gid in gids:
                    all_items.extend(fetch_phase(lg['id'], pid, gid, max_p))
            else:
                all_items.extend(fetch_phase(lg['id'], pid, max_pages=max_p))
        seen = set()
        matches = [m for m in all_items if not (m['id'] in seen or seen.add(m['id']))]
    else:
        # No phase filter — paginate but stop when no known teams
        all_items = fetch_phase(lg['id'], max_pages=max_p, known_teams=known)
        seen = set()
        matches = [m for m in all_items if not (m['id'] in seen or seen.add(m['id']))]

    # Filtriraj samo tekme ki spadajo k tej ligi
    # Za liga3: dodatno filtriraj po znanih ekipah (phase filter je pokvarjen)
    LIGA2_TEAMS = {
        'Voga Grosuplje','Ipros Vrhnika','Celje','Gorica','Hidria','Ježica',
        'LTH Castings','Ljubljana','Plama Pur Ilirska Bistrica','Portorož','Postojna','Slovan'
    }
    LIGA3_TEAMS = {
        'Konjice','Branik Maribor','Bistrica Kety Emmi','Innoduler Dravograd Koroška',
        'Vojnik G7','Elektra Šoštanj','Hrastnik','Vrani Vransko','Kovinarstvo Bučar Miklavž','Nazarje',
        'Leone Ajdovščina','Armicafe Troti','Cedevita Olimpija mladi','Koper',
        'Mesarija Prunk Sežana','Kolpa','Litija','Janče ECP Tactical','Gorenja vas','Tera Tolmin'
    }
    if key == 'liga2':
        matches = [m for m in matches
                   if m['firstTeamName'] in LIGA2_TEAMS and m['secondTeamName'] in LIGA2_TEAMS]
    elif key == 'liga3':
        matches = [m for m in matches
                   if m['firstTeamName'] in LIGA3_TEAMS and m['secondTeamName'] in LIGA3_TEAMS]
    else:
        matches = [m for m in matches
                   if any(c.get('competitionId') == lg['id']
                          for c in m.get('competitions', [{}]))]

    matches.sort(key=lambda m: (m['round'], m.get('dateTime', '')))
    return matches

def load_existing_stats(key):
    path = f"data/{key}_stats.json"
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None

def load_existing_pbp(key):
    path = f"data/{key}_pbp.json"
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f).get('pbp', {})
    return {}

def needs_full_fetch(existing):
    if FORCE_FULL or not existing:
        return True
    updated = datetime.fromisoformat(existing['updatedAt'].replace('Z', '+00:00'))
    age_h = (datetime.now(timezone.utc) - updated).total_seconds() / 3600
    return age_h > 24 * 7

def fetch_stats_incremental(matches, existing_stats):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
    needs = []
    for m in matches:
        if m['status'] != 'FINISHED': continue
        mid = str(m['id'])
        has = mid in existing_stats
        try:
            dt = datetime.fromisoformat(m['dateTime'].replace('Z', '+00:00'))
        except:
            dt = datetime.min.replace(tzinfo=timezone.utc)
        if not has or dt > cutoff:
            needs.append(m)

    if not needs:
        print(f"  Stats: vse shranjeno ({len(existing_stats)})")
        return existing_stats

    print(f"  Stats: fetcham {len(needs)} tekem...")
    stats = dict(existing_stats)
    for i, m in enumerate(needs):
        d = fetch_json(f"{API_BASE}/matches/{m['id']}/stats")
        if d and d.get('data'):
            stats[str(m['id'])] = d['data']
        if (i+1) % 10 == 0:
            print(f"  Stats {i+1}/{len(needs)}")
        time.sleep(0.1)
    return stats

def fetch_pbp_incremental(matches, existing_pbp):
    needs = [m for m in matches
             if m['status'] == 'FINISHED'
             and m.get('fibaLiveStatsUrl')
             and str(m['id']) not in existing_pbp]

    if not needs:
        print(f"  PBP: vse shranjeno ({len(existing_pbp)})")
        return existing_pbp

    print(f"  PBP: fetcham {len(needs)} novih tekem...")
    pbp = dict(existing_pbp)
    for i, m in enumerate(needs):
        fid = m['fibaLiveStatsUrl'].rstrip('/').split('/')[-1]
        d = fetch_json(f"{FIBA_BASE}/{fid}/data.json")
        if d and d.get('pbp'):
            pbp[str(m['id'])] = [
                {k: ev.get(k,'') for k in
                 ('gt','period','periodType','lead','tno','actionType','subType','success','firstName','familyName')}
                for ev in d['pbp']
                if ev.get('actionType') in ('2pt','3pt','freethrow','turnover','assist','rebound','block','steal')
            ]
        if (i+1) % 5 == 0:
            print(f"  PBP {i+1}/{len(needs)}")
        time.sleep(0.4)
    return pbp

def process_league(key, lg):
    print(f"\n--- {lg['name']} ---")
    existing = load_existing_stats(key)

    # Matches — vedno fresh (hitro, samo seznam)
    matches = fetch_all_matches(key, lg)
    finished = [m for m in matches if m['status'] == 'FINISHED']
    live     = [m for m in matches if m['status'] == 'LIVE']
    print(f"  {len(matches)} tekem | {len(finished)} končanih | {len(live)} v živo")
    print(f"  Ekipe: {len(set(m['firstTeamName'] for m in matches))}")

    # Stats
    existing_stats = existing.get('matchStats', {}) if existing else {}
    stats = fetch_stats_incremental(matches, existing_stats)

    now = datetime.now(timezone.utc).isoformat()
    stats_file = f"data/{key}_stats.json"
    with open(stats_file, 'w') as f:
        json.dump({'updatedAt': now, 'league': lg['name'],
                   'allMatches': matches, 'matchStats': stats},
                  f, ensure_ascii=False, separators=(',',':'))
    print(f"  ✅ {stats_file} ({os.path.getsize(stats_file)//1024} KB)")

    # PBP — samo če zahtevano z --pbp flagom
    if FETCH_PBP:
        # Liga3 PBP: FIBA blokira GitHub Actions — preskočimo
        if key == 'liga3':
            print(f"  PBP: liga3 preskočena (FIBA blokira GitHub Actions IP)")
            pbp_file = f"data/{key}_pbp.json"
            if not os.path.exists(pbp_file):
                with open(pbp_file, 'w') as f:
                    json.dump({'updatedAt': now, 'pbp': {}}, f)
        else:
            existing_pbp = load_existing_pbp(key)
            pbp = fetch_pbp_incremental(matches, existing_pbp)
            pbp_file = f"data/{key}_pbp.json"
            with open(pbp_file, 'w') as f:
                json.dump({'updatedAt': now, 'pbp': pbp},
                          f, ensure_ascii=False, separators=(',',':'))
            print(f"  ✅ {pbp_file} ({os.path.getsize(pbp_file)//1024} KB)")
    else:
        print(f"  PBP: preskočen (dodaj --pbp za fetch)")

    # Attendance
    return [
        {'matchId': m['id'], 'round': m['round'], 'date': m.get('dateTime',''),
         'home': m['firstTeamName'], 'away': m['secondTeamName'],
         'homeScore': m.get('firstTeamScore'), 'awayScore': m.get('secondTeamScore'),
         'attendance': m.get('attendance', 0), 'hall': m.get('sportHallName',''),
         'phase': m.get('competitions',[{}])[0].get('competitionPhaseName',''),
         'phaseGroup': m.get('competitions',[{}])[0].get('competitionPhaseGroupName',''),
        }
        for m in finished if m.get('attendance', 0) > 0
    ]

# ── MAIN ──
os.makedirs('data', exist_ok=True)
mode = 'FULL+PBP' if (FORCE_FULL and FETCH_PBP) else ('FULL' if FORCE_FULL else ('PBP' if FETCH_PBP else 'INCREMENTAL'))
print(f"\n=== KZS Fetcher [{mode}] {datetime.now(timezone.utc).isoformat()} ===")

all_att = {}
for key, lg in LEAGUES.items():
    all_att[key] = process_league(key, lg)

with open('data/attendance.json', 'w') as f:
    json.dump({'updatedAt': datetime.now(timezone.utc).isoformat(), 'leagues': all_att},
              f, ensure_ascii=False, separators=(',',':'))
print(f"\n✅ data/attendance.json")
print(f"=== Done! ===")
