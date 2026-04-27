#!/usr/bin/env python3
"""Fetch Liga OTP banka + 2. SKL data using phase IDs to avoid fetching all pages."""

import json, time, urllib.request, os
from datetime import datetime, timezone

API_BASE = "https://api.kzs.si/api/v1/public"
FIBA_BASE = "https://fibalivestats.dcd.shared.geniussports.com/data"
SEASON_ID = 26

LEAGUES = {
    'liga1': {
        'id': 579,
        'name': 'Liga OTP banka',
        # Fetch first page only — liga1 has 135 tekme, fits in 1 page
        'phase_ids': None,
        'max_pages': 1,
    },
    'liga2': {
        'id': 581,
        'name': '2. SKL',
        # Fetch by specific phase IDs to avoid getting 60k+ tekme
        'phase_ids': [5813, 5873, 5874, 5880],
        'max_pages': 1,
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

def fetch_matches_for_phase(comp_id, phase_id):
    """Fetch all matches for a specific phase (paginates if needed)."""
    all_items = []
    page = 1
    while True:
        url = f"{API_BASE}/matches/?competitionId={comp_id}&seasonId={SEASON_ID}&competitionPhaseId={phase_id}&page={page}"
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
    """Fetch single page of matches (for leagues that fit in one page)."""
    url = f"{API_BASE}/matches/?competitionId={comp_id}&seasonId={SEASON_ID}"
    data = fetch_json(url)
    return data.get('data', {}).get('items', []) if data else []

def process_league(key, lg):
    print(f"\n--- {lg['name']} ---")

    if lg['phase_ids']:
        # Fetch by phase IDs — precise, no bloat
        all_matches = []
        for phase_id in lg['phase_ids']:
            items = fetch_matches_for_phase(lg['id'], phase_id)
            print(f"  Phase {phase_id}: {len(items)} tekem")
            all_matches.extend(items)
        # Deduplicate by match ID
        seen = set()
        matches = []
        for m in all_matches:
            if m['id'] not in seen:
                seen.add(m['id'])
                matches.append(m)
        matches.sort(key=lambda m: (m['round'], m.get('dateTime', '')))
        print(f"  Skupaj: {len(matches)} tekem")
    else:
        # Single page fetch for smaller leagues
        matches = fetch_matches_single_page(lg['id'])
        matches.sort(key=lambda m: (m['round'], m.get('dateTime', '')))
        print(f"  {len(matches)} tekem (ena stran)")

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
