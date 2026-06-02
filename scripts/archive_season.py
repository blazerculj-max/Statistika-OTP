#!/usr/bin/env python3
"""
archive_season.py — Arhivira trenutno sezono in posodobi seasons.json manifest.

KAKO DELUJE:
  - Prebere SEASON_ID (privzeto iz okolja ali argumenta).
  - Vsako data/liga{N}_stats.json prekopira v data/liga{N}_stats_s{SEASON_ID}.json.
  - Posodobi data/seasons.json (seznam razpoložljivih sezon).

UPORABA:
  python archive_season.py            # arhivira trenutno (SEASON_ID iz fetch_data.py)
  python archive_season.py 26         # eksplicitno arhivira kot sezono 26

KDAJ ZAGNATI:
  - Ob koncu sezone, PREDEN posodobiš SEASON_ID v fetch_data.py za novo sezono.
  - Ali kadarkoli, da ustvariš posnetek trenutne sezone.

Po arhiviranju aplikacija samodejno ponudi izbiro sezone (bere seasons.json).
"""
import json
import os
import sys
import shutil

# Mapiranje SEASON_ID -> ime sezone (uskladi z aplikacijo)
SEASON_NAMES = {
    22: '2018/19', 23: '2019/20', 24: '2023/24', 25: '2024/25', 26: '2025/26',
    27: '2026/27', 28: '2027/28', 29: '2028/29', 30: '2029/30',
}
LEAGUES = ['liga1', 'liga2', 'liga3']
LEAGUE_NAMES = {'liga1': 'Liga OTP banka', 'liga2': '2. SKL', 'liga3': '3. SKL'}


def get_season_id():
    if len(sys.argv) > 1:
        return int(sys.argv[1])
    # Poskusi prebrati iz fetch_data.py
    try:
        with open('scripts/fetch_data.py') as f:
            for line in f:
                if line.strip().startswith('SEASON_ID'):
                    return int(line.split('=')[1].strip().split()[0])
    except Exception:
        pass
    try:
        with open('fetch_data.py') as f:
            for line in f:
                if line.strip().startswith('SEASON_ID'):
                    return int(line.split('=')[1].strip().split()[0])
    except Exception:
        pass
    raise SystemExit("Ne najdem SEASON_ID — podaj kot argument: python archive_season.py 26")


def main():
    sid = get_season_id()
    sname = SEASON_NAMES.get(sid, f'Sezona {sid}')
    print(f"Arhiviram sezono {sid} ({sname})...")

    archived = []
    for key in LEAGUES:
        src = f"data/{key}_stats.json"
        if not os.path.exists(src):
            print(f"  ! {src} ne obstaja — preskačem")
            continue
        dst = f"data/{key}_stats_s{sid}.json"
        shutil.copy2(src, dst)
        size = os.path.getsize(dst) // 1024
        print(f"  ✓ {dst} ({size} KB)")
        archived.append(key)

    # Posodobi seasons.json manifest
    manifest_path = "data/seasons.json"
    manifest = {}
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
        except Exception:
            manifest = {}

    seasons = manifest.get('seasons', {})
    seasons[str(sid)] = {
        'id': sid,
        'name': sname,
        'leagues': archived,
        'files': {key: f"{key}_stats_s{sid}.json" for key in archived},
    }
    manifest['seasons'] = seasons
    manifest['current'] = sid  # trenutna sezona

    with open(manifest_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    print(f"  ✓ {manifest_path} posodobljen")
    print(f"\nSezone v arhivu: {sorted(seasons.keys())}")
    print("Končano. Zdaj lahko posodobiš SEASON_ID za novo sezono.")


if __name__ == "__main__":
    main()
