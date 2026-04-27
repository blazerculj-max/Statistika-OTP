#!/usr/bin/env python3
"""Fetch Liga OTP banka + 2. SKL data including all phases."""

import json, time, urllib.request, os
from datetime import datetime, timezone

API_BASE = "https://api.kzs.si/api/v1/public"
FIBA_BASE = "https://fibalivestats.dcd.shared.geniussports.com/data"
SEASON_ID = 26

LEAGUES = {
    'liga1': {
        'id': 579,
        'name': 'Liga OTP banka',
        'phase_ids': None,  # fetch all phases
    },
    'liga2': {
        'id': 581,
        'name': '2. SKL',
        'phase_ids': [5813, 5873, 5874, 5880],  # redni + obstanek + cetrtfinale + polfinale
    },
}

def fetch_json(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            print(f"  Err: {e}")
            time.sleep(i + 1)
    return None

def process_league(key, lg):
    print(f"\n--- {lg['name']} ---")
    data = fetch_json(f"{API_BASE}/matches/?competitionId={lg['id']}&seasonId={SEASON_ID}")
    if not data:
        print("  FAILED"); return

    all_matches = sorted(data['data']['items'],
                         key=lambda m: (m['round'], m.get('dateTime', '')))

    # Filter by phase if specified
    if lg['phase_ids']:
        matches = [m for m in all_matches
                   if any(c.get('competitionPhaseId') in lg['phase_ids']
                          for c in m.get('competitions', []))]
        print(f"  {len(matches)}/{len(all_matches)} tekem (filtrirano po fazah)")
    else:
        matches = all_matches
        print(f"  {len(matches)} tekem")

    finished = [m for m in matches if m['status'] == 'FINISHED']
    print(f"  {len(finished)} odigranih")

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
    stats_file = f"data/{key}_stats.json"
    with open(stats_file, 'w') as f:
        json.dump({'updatedAt': now, 'allMatches': matches, 'matchStats': stats},
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

os.makedirs('data', exist_ok=True)
print(f"=== KZS Fetcher {datetime.now(timezone.utc).isoformat()} ===")
for key, lg in LEAGUES.items():
    process_league(key, lg)
print("\n=== Done! ===")
