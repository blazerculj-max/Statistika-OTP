#!/usr/bin/env python3
"""
archive_season.py — Arhivira CELOTNO sezono in posodobi seasons.json manifest.

KAJ ARHIVIRA:
  - Lige:      data/liga{1,2,3}_{stats,pregled,pbp}.json  →  ..._s{SID}.json
  - Obisk:     data/attendance.json                       →  attendance_s{SID}.json
  - 3x3:       data/drzavc_3x3.json                       →  drzavc_3x3_s{SID}.json
  - Mladinci:  data/{mladi,hks,srb,bg}_manifest.json + vse datoteke, na katere
               manifest kaže. V arhivskem manifestu se poti prepišejo na _s{SID}.
  - Transferji (data/tr_*.json): SAMO z zastavico --transfers. Brez nje se ne
    kopirajo — nimajo aktivnega fetcherja, zato so datoteke že same po sebi
    zamrznjen posnetek sezone.

UPORABA:
  python scripts/archive_season.py             # SEASON_ID prebere iz fetch_data.py
  python scripts/archive_season.py 26          # eksplicitno
  python scripts/archive_season.py 26 --dry-run
  python scripts/archive_season.py 26 --transfers

KDAJ ZAGNATI:
  - Ob koncu sezone, PREDEN posodobiš SEASON_ID v fetch_data.py za novo sezono.
  - Ali kadarkoli, da osvežiš posnetek trenutne sezone (skript je idempotenten).

Po arhiviranju aplikacija samodejno ponudi izbiro sezone (bere seasons.json).
"""
import json
import os
import re
import shutil
import sys
from datetime import datetime, timezone

DATA = "data"

# Mapiranje SEASON_ID -> ime sezone (uskladi s SEASON_NAMES v index.html)
SEASON_NAMES = {
    22: '2021/22', 23: '2022/23', 24: '2023/24', 25: '2024/25', 26: '2025/26',
    27: '2026/27', 28: '2027/28', 29: '2028/29', 30: '2029/30',
}
LEAGUES = ['liga1', 'liga2', 'liga3']
LEAGUE_NAMES = {'liga1': 'Liga OTP banka', 'liga2': '2. SKL', 'liga3': '3. SKL'}

# Vrste datotek na ligo (stats je obvezen, ostalo neobvezno)
LEAGUE_KINDS = ['stats', 'pregled', 'pbp']

# Samostojne datoteke — kopija 1:1
SIMPLE_FILES = ['attendance.json', 'drzavc_3x3.json']

# Manifest skupine: manifest + vse datoteke iz .ages[*].file
MANIFEST_GROUPS = {
    'mladi': 'mladi_manifest.json',   # Slovenija (KZS)
    'hks':   'hks_manifest.json',     # Hrvaška
    'srb':   'srb_manifest.json',     # Srbija
    'bg':    'bg_manifest.json',      # Bolgarija
}

DRY_RUN = '--dry-run' in sys.argv
WITH_TRANSFERS = '--transfers' in sys.argv

# Prepozna že arhivirane datoteke (…_s26.json), da jih ne arhiviramo znova
ARCHIVED_RE = re.compile(r'_s\d+\.json$')


def archived_name(fname, sid):
    """foo.json → foo_s26.json"""
    stem = fname[:-5] if fname.endswith('.json') else fname
    return f"{stem}_s{sid}.json"


# Sezona 2025/26 ima id 26, 2026/27 id 27 … → id = začetna letnica - 1999.
def _year_to_sid(year):
    return int(year) - 1999


def _read_head(path, n=4096):
    try:
        with open(path, encoding='utf-8') as f:
            return f.read(n)
    except Exception:
        return ''


def source_season(name):
    """
    Kateri sezoni pripada data/<name>? Vrne id ali None, če je ni mogoče ugotoviti.
      1. eksplicitni "seasonId"                (liga*_stats, liga*_pbp, mladi_u*, po novem tudi pregled/attendance)
      2. razpon v "season": "2025-26" / "2025/2026"   (hks/srb/bg/mladi manifesti)
      3. pregled in attendance brez oznake podedujeta sezono od pripadajočega _stats
    """
    head = _read_head(os.path.join(DATA, name))
    m = re.search(r'"seasonId"\s*:\s*(\d+)', head)
    if m:
        return int(m.group(1))
    m = re.search(r'"season"\s*:\s*"(\d{4})[-/]\d{2,4}"', head)
    if m:
        return _year_to_sid(m.group(1))
    # starejše datoteke brez oznake: podedujmo od pripadajoče _stats datoteke
    sib = None
    if name.endswith('_pregled.json'):
        sib = name.replace('_pregled.json', '_stats.json')
    elif name == 'attendance.json':
        sib = next((f"{k}_stats.json" for k in LEAGUES
                    if os.path.exists(os.path.join(DATA, f"{k}_stats.json"))), None)
    if sib:
        m = re.search(r'"seasonId"\s*:\s*(\d+)', _read_head(os.path.join(DATA, sib)))
        if m:
            return int(m.group(1))
    return None


def copy(src_name, sid, log):
    """Kopira data/<src_name> v arhivsko različico. Vrne ime arhiva ali None."""
    src = os.path.join(DATA, src_name)
    if not os.path.exists(src):
        log.append(f"  – {src_name} ne obstaja — preskačem")
        return None

    # VARNOSTNA ZAPORA: ko se sezona prelomi, žive datoteke pripadajo že NOVI
    # sezoni. Brez tega bi arhiviranje stare sezone povozilo njen arhiv z
    # novimi (praznimi) podatki. Datoteke z neujemajočo se sezono preskočimo.
    dst_name = archived_name(src_name, sid)
    src_sid = source_season(src_name)
    if src_sid is not None and src_sid != sid:
        # Arhiv, ki že obstaja, ostane veljaven in ohrani mesto v manifestu.
        if os.path.exists(os.path.join(DATA, dst_name)):
            log.append(f"  = {dst_name} že arhiviran — ohranjam "
                       f"(živa datoteka je zdaj sezona {src_sid})")
            return dst_name
        log.append(f"  ⛔ {src_name} je sezona {src_sid}, ne {sid} — NE arhiviram "
                   f"(arhiva ni, živa datoteka pripada drugi sezoni)")
        return None

    dst = os.path.join(DATA, dst_name)
    size = os.path.getsize(src) // 1024
    if DRY_RUN:
        log.append(f"  » {dst_name} ({size} KB) [dry-run]")
    else:
        shutil.copy2(src, dst)
        log.append(f"  ✓ {dst_name} ({size} KB)")
    return dst_name


def archive_manifest_group(group, manifest_name, sid, log):
    """
    Kopira manifest + vse datoteke, na katere kaže (ages[*].file), in v
    arhivskem manifestu prepiše 'file' na arhivska imena.
    Vrne ime arhivskega manifesta ali None.
    """
    src = os.path.join(DATA, manifest_name)
    if not os.path.exists(src):
        log.append(f"  – {manifest_name} ne obstaja — preskačem")
        return None
    try:
        with open(src, encoding='utf-8') as f:
            manifest = json.load(f)
    except Exception as e:
        log.append(f"  ! {manifest_name} ni berljiv ({e}) — preskačem")
        return None

    ages = manifest.get('ages', {})
    for age, meta in ages.items():
        fname = meta.get('file')
        if not fname:
            continue
        new_name = copy(fname, sid, log)
        if new_name:
            meta['file'] = new_name   # arhivski manifest kaže na arhivske datoteke

    manifest['archivedFrom'] = manifest_name
    manifest['seasonId'] = sid

    dst_name = archived_name(manifest_name, sid)
    if DRY_RUN:
        log.append(f"  » {dst_name} (manifest) [dry-run]")
    else:
        with open(os.path.join(DATA, dst_name), 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        log.append(f"  ✓ {dst_name} (manifest)")
    return dst_name


def archive_transfers(sid, log):
    """Kopira tr_*.json (razen že arhiviranih). Vrne število kopij."""
    names = sorted(n for n in os.listdir(DATA)
                   if n.startswith('tr_') and n.endswith('.json')
                   and not ARCHIVED_RE.search(n))
    for n in names:
        copy(n, sid, log)
    return len(names)


def get_season_id():
    """Sezona iz argumenta, sicer 'current' iz seasons.json (vzdržuje ga fetcher)."""
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    if args:
        return int(args[0])
    try:
        with open(os.path.join(DATA, 'seasons.json'), encoding='utf-8') as f:
            cur = json.load(f).get('current')
        if cur is not None:
            return int(cur)
    except Exception:
        pass
    raise SystemExit("Ne najdem trenutne sezone (data/seasons.json → 'current'). "
                     "Podaj kot argument: python scripts/archive_season.py 26")


def main():
    if not os.path.isdir(DATA):
        raise SystemExit(f"Mapa {DATA}/ ne obstaja — poženi skript iz korena repozitorija.")

    sid = get_season_id()
    sname = SEASON_NAMES.get(sid, f'Sezona {sid}')
    print(f"Arhiviram sezono {sid} ({sname}){' [DRY RUN]' if DRY_RUN else ''}...\n")

    assets = {}
    log = []

    # ── 1. Lige ──
    print("Lige:")
    archived_leagues = []
    league_assets = {}
    for key in LEAGUES:
        log.clear()
        kinds = {}
        for kind in LEAGUE_KINDS:
            name = copy(f"{key}_{kind}.json", sid, log)
            if name:
                kinds[kind] = name
        if 'stats' in kinds:
            archived_leagues.append(key)
            league_assets[key] = kinds
        for line in log:
            print(line)
    assets['leagues'] = league_assets

    # ── 2. Samostojne datoteke ──
    print("\nSamostojne datoteke:")
    log.clear()
    for fname in SIMPLE_FILES:
        name = copy(fname, sid, log)
        if name:
            assets[fname[:-5]] = name
    for line in log:
        print(line)

    # ── 3. Mladinski manifesti ──
    print("\nMladinci (manifest + starosti):")
    manifests = {}
    for group, manifest_name in MANIFEST_GROUPS.items():
        log.clear()
        print(f" {group}:")
        name = archive_manifest_group(group, manifest_name, sid, log)
        if name:
            manifests[group] = name
        for line in log:
            print(line)
    assets['manifests'] = manifests

    # ── 4. Transferji (neobvezno) ──
    if WITH_TRANSFERS:
        print("\nTransferji:")
        log.clear()
        n = archive_transfers(sid, log)
        for line in log:
            print(line)
        assets['transfers'] = {'count': n, 'manifest': archived_name('tr_manifest.json', sid)}
    else:
        print("\nTransferji: preskočeni (dodaj --transfers). Datoteke tr_*.json"
              "\n  nimajo aktivnega fetcherja, zato so že zamrznjen posnetek sezone.")

    # ── 5. seasons.json manifest ──
    manifest_path = os.path.join(DATA, "seasons.json")
    manifest = {}
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, encoding='utf-8') as f:
                manifest = json.load(f)
        except Exception:
            manifest = {}

    seasons = manifest.get('seasons', {})
    entry = seasons.get(str(sid), {})
    entry.update({
        'id': sid,
        'name': sname,
        'leagues': archived_leagues,
        # 'files' ohranjeno zaradi združljivosti s starejšim frontendom
        'files': {k: league_assets[k]['stats'] for k in archived_leagues},
        'assets': assets,
        'archivedAt': datetime.now(timezone.utc).isoformat(timespec='seconds'),
    })
    seasons[str(sid)] = entry
    manifest['seasons'] = seasons
    manifest.setdefault('current', sid)

    if DRY_RUN:
        print(f"\n» {manifest_path} [dry-run]")
        print(json.dumps(entry, ensure_ascii=False, indent=2)[:1200])
    else:
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        print(f"\n✓ {manifest_path} posodobljen")

    print(f"\nSezone v arhivu: {sorted(seasons.keys(), key=int)}")
    print(f"Trenutna (manifest.current): {manifest.get('current')}")
    if not DRY_RUN:
        print("\nKončano. Zdaj lahko posodobiš SEASON_ID za novo sezono.")


if __name__ == "__main__":
    main()
