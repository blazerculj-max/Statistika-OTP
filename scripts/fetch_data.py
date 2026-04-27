#!/usr/bin/env python3
"""
Fetch Liga OTP banka data from KZS API and FIBA LiveStats.
Saves processed data to data/ directory for use by the web app.
"""

import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

API_BASE = "https://api.kzs.si/api/v1/public"
FIBA_BASE = "https://fibalivestats.dcd.shared.geniussports.com/data"
COMP_ID = 579
SEASON_ID = 26

def fetch_json(url, retries=3, delay=1):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (compatible; DataFetcher/1.0)'
            })
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            print(f"  Error fetching {url}: {e}")
            if i < retries - 1:
                time.sleep(delay * (i + 1))
    return None

def fetch_all_matches():
    print("Fetching match list...")
    url = f"{API_BASE}/matches/?competitionId={COMP_ID}&seasonId={SEASON_ID}"
    data = fetch_json(url)
    if not data:
        raise Exception("Failed to fetch matches")
    matches = sorted(data['data']['items'],
                     key=lambda m: (m['round'], m.get('dateTime', '')))
    print(f"  Found {len(matches)} matches")
    return matches

def fetch_match_stats(matches):
    print("Fetching match stats...")
    finished = [m for m in matches if m['status'] == 'FINISHED']
    print(f"  {len(finished)} finished matches to fetch")
    
    stats = {}
    for i, m in enumerate(finished):
        url = f"{API_BASE}/matches/{m['id']}/stats"
        data = fetch_json(url)
        if data and data.get('data'):
            stats[str(m['id'])] = data['data']
        if (i + 1) % 10 == 0:
            print(f"  Stats: {i+1}/{len(finished)}")
        time.sleep(0.1)  # be nice to the API
    
    print(f"  Fetched stats for {len(stats)} matches")
    return stats

def get_fiba_id(match):
    url = match.get('fibaLiveStatsUrl', '')
    if not url:
        return None
    parts = url.rstrip('/').split('/')
    return parts[-1] if parts else None

def fetch_pbp(matches):
    print("Fetching play-by-play data...")
    finished = [m for m in matches if m['status'] == 'FINISHED' and m.get('fibaLiveStatsUrl')]
    print(f"  {len(finished)} matches with FIBA data")
    
    pbp_data = {}
    for i, m in enumerate(finished):
        fiba_id = get_fiba_id(m)
        if not fiba_id:
            continue
        url = f"{FIBA_BASE}/{fiba_id}/data.json"
        data = fetch_json(url)
        if data and data.get('pbp'):
            # Store only fields needed for nerd stats (reduce file size)
            slim_pbp = []
            for ev in data['pbp']:
                if ev.get('actionType') in ('2pt', '3pt', 'freethrow', 'turnover', 'assist'):
                    slim_pbp.append({
                        'gt': ev.get('gt', ''),
                        'period': ev.get('period', 1),
                        'periodType': ev.get('periodType', 'REGULAR'),
                        'lead': ev.get('lead', 0),
                        'tno': ev.get('tno', 0),
                        'actionType': ev.get('actionType', ''),
                        'subType': ev.get('subType', ''),
                        'success': ev.get('success', 1),
                        'firstName': ev.get('internationalFirstName', ev.get('firstName', '')),
                        'familyName': ev.get('internationalFamilyName', ev.get('familyName', '')),
                    })
            pbp_data[str(m['id'])] = slim_pbp
        if (i + 1) % 10 == 0:
            print(f"  PBP: {i+1}/{len(finished)}")
        time.sleep(0.3)
    
    print(f"  Fetched PBP for {len(pbp_data)} matches")
    return pbp_data

def main():
    import os
    os.makedirs('data', exist_ok=True)

    now = datetime.now(timezone.utc).isoformat()
    print(f"\n=== Liga OTP banka Data Fetcher ===")
    print(f"Time: {now}\n")

    # 1. Fetch matches
    matches = fetch_all_matches()

    # 2. Fetch match stats
    stats = fetch_match_stats(matches)

    # 3. Save stats + matches
    stats_payload = {
        'updatedAt': now,
        'allMatches': matches,
        'matchStats': stats,
    }
    with open('data/stats.json', 'w', encoding='utf-8') as f:
        json.dump(stats_payload, f, ensure_ascii=False, separators=(',', ':'))
    size_kb = os.path.getsize('data/stats.json') / 1024
    print(f"\nSaved data/stats.json ({size_kb:.0f} KB)")

    # 4. Fetch PBP
    pbp = fetch_pbp(matches)
    pbp_payload = {
        'updatedAt': now,
        'pbp': pbp,
    }
    with open('data/pbp.json', 'w', encoding='utf-8') as f:
        json.dump(pbp_payload, f, ensure_ascii=False, separators=(',', ':'))
    size_kb = os.path.getsize('data/pbp.json') / 1024
    print(f"Saved data/pbp.json ({size_kb:.0f} KB)")

    print(f"\n=== Done! ===")

if __name__ == '__main__':
    main()
