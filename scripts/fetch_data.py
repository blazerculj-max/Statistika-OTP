#!/usr/bin/env python3
"""Fetch Liga OTP banka + 2. SKL data. Save to data/ directory."""

import json, time, urllib.request, os
from datetime import datetime, timezone

API_BASE = "https://api.kzs.si/api/v1/public"
FIBA_BASE = "https://fibalivestats.dcd.shared.geniussports.com/data"
SEASON_ID = 26
LEAGUES = {
    'liga1': {'id': 579, 'name': 'Liga OTP banka'},
    'liga2': {'id': 581, 'name': '2. SKL'},
}

def fetch_json(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            print(f"  Err: {e}"); time.sleep(i+1)
    return None

def process_league(key, lg):
    print(f"\n--- {lg['name']} ---")
    data = fetch_json(f"{API_BASE}/matches/?competitionId={lg['id']}&seasonId={SEASON_ID}")
    matches = sorted(data['data']['items'], key=lambda m: (m['round'], m.get('dateTime','')))
    finished = [m for m in matches if m['status'] == 'FINISHED']
    print(f"  {len(finished)}/{len(matches)} odigranih")

    stats = {}
    for i, m in enumerate(finished):
        d = fetch_json(f"{API_BASE}/matches/{m['id']}/stats")
        if d and d.get('data'): stats[str(m['id'])] = d['data']
        if (i+1) % 10 == 0: print(f"  Stats {i+1}/{len(finished)}")
        time.sleep(0.1)

    now = datetime.now(timezone.utc).isoformat()
    with open(f"data/{key}_stats.json", 'w') as f:
        json.dump({'updatedAt':now,'allMatches':matches,'matchStats':stats}, f, ensure_ascii=False, separators=(',',':'))
    print(f"  Saved {key}_stats.json ({os.path.getsize(f'data/{key}_stats.json')//1024} KB)")

    pbp = {}
    fiba_matches = [m for m in finished if m.get('fibaLiveStatsUrl')]
    for i, m in enumerate(fiba_matches):
        fid = m['fibaLiveStatsUrl'].rstrip('/').split('/')[-1]
        d = fetch_json(f"{FIBA_BASE}/{fid}/data.json")
        if d and d.get('pbp'):
            pbp[str(m['id'])] = [
                {k: ev.get(k,'') for k in ('gt','period','periodType','lead','tno','actionType','subType','success','firstName','familyName')}
                for ev in d['pbp'] if ev.get('actionType') in ('2pt','3pt','freethrow','turnover','assist')
            ]
        if (i+1) % 10 == 0: print(f"  PBP {i+1}/{len(fiba_matches)}")
        time.sleep(0.3)

    with open(f"data/{key}_pbp.json", 'w') as f:
        json.dump({'updatedAt':now,'pbp':pbp}, f, ensure_ascii=False, separators=(',',':'))
    print(f"  Saved {key}_pbp.json ({os.path.getsize(f'data/{key}_pbp.json')//1024} KB)")

os.makedirs('data', exist_ok=True)
print(f"=== KZS Data Fetcher {datetime.now(timezone.utc).isoformat()} ===")
for key, lg in LEAGUES.items():
    process_league(key, lg)
print("=== Done! ===")
