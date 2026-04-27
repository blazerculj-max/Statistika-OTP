#!/usr/bin/env python3
"""
Fetch KZS basketball data for all 3 leagues.
Saves to data/ directory for use by the web app.
"""

import json, time, urllib.request, os
from datetime import datetime, timezone

API_BASE = "https://api.kzs.si/api/v1/public"
FIBA_BASE = "https://fibalivestats.dcd.shared.geniussports.com/data"
SEASON_ID = 26

LEAGUES = {
    'liga1': {
        'id': 579,
        'name': 'Liga OTP banka',
        'short': '1. SKL',
        'phase_ids': None,   # single page, all phases
        'group_ids': None,
    },
    'liga2': {
        'id': 581,
        'name': '2. SKL',
        'short': '2. SKL',
        'phase_ids': [5813, 5873, 5874, 5880],
        'group_ids': None,
    },
    'liga3': {
        'id': 582,
        'name': '3. SKL',
        'short': '3. SKL',
        # Redni del (Vzhod+Zahod) + Četrtfinale + 1.krog končnica
        'phase_ids': [5814, 5819, 5820],
        'group_ids': None,  # fetch all groups within phase
    },
}

def fetch_json(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            print(f"  Err ({i+1}/{retries}): {e}")
            time.sleep(i + 1)
    return None

def fetch_matches_for_phase(comp_id, phase_id, group_id=None):
    """Fetch all matches for a specific phase, optionally filtered by group."""
    all_items = []
    page = 1
    while True:
        url = f"{API_BASE}/matches/?competitionId={comp_id}&seasonId={SEASON_ID}&competitionPhaseId={phase_id}"
        if group_id:
            url += f"&competitionPhaseGroupId={group_id}"
        if page > 1:
            url += f"&page={page}"
        data = fetch_json(url)
        items = data.get('data', {}).get('items', []) if data else []
        if not items:
            break
        all_items.extend(items)
        if len(items) < 150:
            break
        page += 1
        time.sleep(0.1)
    return all_items

def fetch_matches_single_page(comp_id):
    """Fetch single page (for leagues that fit in one page)."""
    url = f"{API_BASE}/matches/?competitionId={comp_id}&seasonId={SEASON_ID}"
    data = fetch_json(url)
    return data.get('data', {}).get('items', []) if data else []

def process_league(key, lg):
    print(f"\n--- {lg['name']} ---")

    if lg['phase_ids']:
        # Fetch by phase ID
        all_matches = []
        for phase_id in lg['phase_ids']:
            items = fetch_matches_for_phase(lg['id'], phase_id)
            phase_name = items[0]['competitions'][0].get('competitionPhaseName', '?') if items else '?'
            print(f"  Phase {phase_id} ({phase_name}): {len(items)} tekem")
            all_matches.extend(items)
        # Deduplicate
        seen = set()
        matches = []
        for m in all_matches:
            if m['id'] not in seen:
                seen.add(m['id'])
                matches.append(m)
        matches.sort(key=lambda m: (m['round'], m.get('dateTime', '')))
        print(f"  Skupaj: {len(matches)} tekem")
    else:
        matches = fetch_matches_single_page(lg['id'])
        matches.sort(key=lambda m: (m['round'], m.get('dateTime', '')))
        print(f"  {len(matches)} tekem (ena stran)")

    finished = [m for m in matches if m['status'] == 'FINISHED']
    live = [m for m in matches if m['status'] == 'LIVE']
    print(f"  {len(finished)} odigranih, {len(live)} v živo")

    # Attendance stats
    with_att = [m for m in finished if m.get('attendance', 0) > 0]
    print(f"  {len(with_att)}/{len(finished)} tekem z obiskoma")

    # Fetch stats
    stats = {}
    for i, m in enumerate(finished):
        d = fetch_json(f"{API_BASE}/matches/{m['id']}/stats")
        if d and d.get('data'):
            stats[str(m['id'])] = d['data']
        if (i + 1) % 10 == 0:
            print(f"  Stats {i+1}/{len(finished)}")
        time.sleep(0.1)

    now = datetime.now(timezone.utc).isoformat()

    # Save stats
    stats_file = f"data/{key}_stats.json"
    with open(stats_file, 'w') as f:
        json.dump({'updatedAt': now, 'league': lg['name'], 'allMatches': matches, 'matchStats': stats},
                  f, ensure_ascii=False, separators=(',', ':'))
    print(f"  Saved {stats_file} ({os.path.getsize(stats_file)//1024} KB)")

    # Fetch PBP
    pbp = {}
    fiba_matches = [m for m in finished if m.get('fibaLiveStatsUrl')]
    for i, m in enumerate(fiba_matches):
        fid = m['fibaLiveStatsUrl'].rstrip('/').split('/')[-1]
        d = fetch_json(f"{FIBA_BASE}/{fid}/data.json")
        if d and d.get('pbp'):
            pbp[str(m['id'])] = [
                {k: ev.get(k, '') for k in
                 ('gt','period','periodType','lead','tno','actionType','subType','success','firstName','familyName')}
                for ev in d['pbp']
                if ev.get('actionType') in ('2pt','3pt','freethrow','turnover','assist')
            ]
        if (i + 1) % 10 == 0:
            print(f"  PBP {i+1}/{len(fiba_matches)}")
        time.sleep(0.3)

    pbp_file = f"data/{key}_pbp.json"
    with open(pbp_file, 'w') as f:
        json.dump({'updatedAt': now, 'pbp': pbp}, f, ensure_ascii=False, separators=(',', ':'))
    print(f"  Saved {pbp_file} ({os.path.getsize(pbp_file)//1024} KB)")

    # Save attendance separately for obiski app
    att_records = [
        {
            'matchId': m['id'],
            'round': m['round'],
            'date': m.get('dateTime', ''),
            'home': m['firstTeamName'],
            'away': m['secondTeamName'],
            'homeScore': m.get('firstTeamScore'),
            'awayScore': m.get('secondTeamScore'),
            'attendance': m.get('attendance', 0),
            'hall': m.get('sportHallName', ''),
            'phase': m.get('competitions', [{}])[0].get('competitionPhaseName', 'Redni del'),
            'phaseGroup': m.get('competitions', [{}])[0].get('competitionPhaseGroupName', ''),
        }
        for m in finished if m.get('attendance', 0) > 0
    ]
    return att_records

os.makedirs('data', exist_ok=True)
print(f"=== KZS Fetcher {datetime.now(timezone.utc).isoformat()} ===")

all_attendance = {}
for key, lg in LEAGUES.items():
    att = process_league(key, lg)
    all_attendance[key] = att

# Save combined attendance
att_file = 'data/attendance.json'
with open(att_file, 'w') as f:
    json.dump({
        'updatedAt': datetime.now(timezone.utc).isoformat(),
        'leagues': all_attendance
    }, f, ensure_ascii=False, separators=(',', ':'))
print(f"\nSaved {att_file} ({os.path.getsize(att_file)//1024} KB)")
print("\n=== Done! ===")
