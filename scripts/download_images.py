#!/usr/bin/env python3
"""
download_images.py — Inkrementalno prenese KZS slike (fotografije igralcev +
logotipi ekip) v lokalni repo, da jih kartice lahko uporabijo brez CORS težav.

INKREMENTALNA LOGIKA:
  - Prvic: prenese vse slike (full).
  - Nato: prenese SAMO nove ali spremenjene slike.
    * Nov igralec / nova ekipa  -> nov UUID -> prenese.
    * Spremenjena slika (KZS)    -> zazna prek ETag / velikosti -> prenese.
    * Nespremenjena slika        -> preskoci (hitro).
  - Opcijsko pocisti slike, ki jih ni vec v podatkih (--cleanup).

Manifest (data/images/_manifest.json) sledi vsakemu UUID-ju:
  { uuid: {size, etag, downloaded_at} }

Uporaba:
  python download_images.py             # inkrementalno
  python download_images.py --cleanup   # + pobrise osirotele slike
  python download_images.py --force     # znova prenese vse (ignorira manifest)

Pozeni PO fetch_data.py (rabi data/*_stats.json).
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error

IMAGES_DIR = "data/images"
MANIFEST_PATH = os.path.join(IMAGES_DIR, "_manifest.json")
KZS_BASE = "https://api.kzs.si/public/images/"
LEAGUES = ["liga1", "liga2", "liga3"]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
    'Accept-Language': 'sl-SI,sl;q=0.9,en;q=0.8',
    'Referer': 'https://www.kzs.si/',
}


def load_manifest():
    if os.path.exists(MANIFEST_PATH):
        try:
            with open(MANIFEST_PATH, encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_manifest(manifest):
    with open(MANIFEST_PATH, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=0)


def collect_uuids():
    """Pobere vse photoUuid (igralci) in logoUuid (ekipe) iz stats datotek."""
    uuids = {}
    for key in LEAGUES:
        stats_path = f"data/{key}_stats.json"
        if not os.path.exists(stats_path):
            print(f"  ! {stats_path} ne obstaja - preskacem")
            continue
        with open(stats_path, encoding='utf-8') as f:
            data = json.load(f)

        for m in data.get('allMatches', []):
            for k in ('firstTeamLogoUuid', 'secondTeamLogoUuid'):
                if m.get(k):
                    uuids[m[k]] = 'logo'

        for mid, md in data.get('matchStats', {}).items():
            if not md:
                continue
            for side in ('firstTeam', 'secondTeam'):
                team = md.get(side)
                if not team:
                    continue
                for ps in team.get('playerStats', []):
                    if ps.get('photoUuid'):
                        uuids[ps['photoUuid']] = 'photo'
    return uuids


def head_info(uuid):
    """Lahek HTTP HEAD - vrne (etag, size) brez prenosa cele slike."""
    url = KZS_BASE + uuid
    try:
        req = urllib.request.Request(url, headers=HEADERS, method='HEAD')
        with urllib.request.urlopen(req, timeout=15) as r:
            etag = r.headers.get('ETag', '') or ''
            size = r.headers.get('Content-Length', '') or ''
            return etag.strip('"'), size
    except Exception:
        return None, None


def download_one(uuid, retries=3):
    """Prenese sliko. Vrne (uspeh, etag, size)."""
    out_path = os.path.join(IMAGES_DIR, f"{uuid}.png")
    url = KZS_BASE + uuid
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=25) as r:
                data = r.read()
                etag = (r.headers.get('ETag', '') or '').strip('"')
            if len(data) < 200:
                return False, None, None
            with open(out_path, 'wb') as f:
                f.write(data)
            return True, etag, str(len(data))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False, None, None
            time.sleep(i + 1)
        except Exception:
            time.sleep(i + 1)
    return False, None, None


def main():
    force = '--force' in sys.argv
    cleanup = '--cleanup' in sys.argv

    os.makedirs(IMAGES_DIR, exist_ok=True)
    manifest = {} if force else load_manifest()

    print("Zbiram UUID-je iz stats datotek...")
    uuids = collect_uuids()
    photos = sum(1 for v in uuids.values() if v == 'photo')
    logos = sum(1 for v in uuids.values() if v == 'logo')
    print(f"  Fotografije igralcev: {photos}")
    print(f"  Logotipi ekip: {logos}")
    print(f"  Skupaj unikatnih: {len(uuids)}")
    print(f"  Ze v manifestu: {len(manifest)}")
    print(f"  Nacin: {'FULL (--force)' if force else 'INKREMENTALNO'}")
    print()

    new_cnt, changed_cnt, skip_cnt, fail_cnt = 0, 0, 0, 0
    total = len(uuids)

    for n, uuid in enumerate(sorted(uuids), 1):
        out_path = os.path.join(IMAGES_DIR, f"{uuid}.png")
        file_exists = os.path.exists(out_path) and os.path.getsize(out_path) > 200
        entry = manifest.get(uuid)

        # 1) Popolnoma nov UUID -> prenesi
        if not entry or not file_exists:
            ok, etag, size = download_one(uuid)
            if ok:
                manifest[uuid] = {'size': size, 'etag': etag, 'downloaded_at': int(time.time())}
                new_cnt += 1
                print(f"  [{n}/{total}] + NOV    {uuid[:8]}...")
            else:
                fail_cnt += 1
                print(f"  [{n}/{total}] x NAPAKA {uuid[:8]}...")
            time.sleep(0.15)
            continue

        # 2) Obstaja - preveri ce se je spremenila (HEAD)
        etag, size = head_info(uuid)
        same = False
        if etag and entry.get('etag'):
            same = (etag == entry['etag'])
        elif size and entry.get('size'):
            same = (size == entry['size'])
        else:
            same = True  # ne moremo preveriti -> predpostavi nespremenjeno

        if same:
            skip_cnt += 1
            continue

        # 3) Spremenjena -> znova prenesi
        ok, new_etag, new_size = download_one(uuid)
        if ok:
            manifest[uuid] = {'size': new_size, 'etag': new_etag, 'downloaded_at': int(time.time())}
            changed_cnt += 1
            print(f"  [{n}/{total}] ~ POSOD. {uuid[:8]}...")
        else:
            fail_cnt += 1
        time.sleep(0.15)

    # 4) Cleanup
    if cleanup:
        current = set(uuids)
        for fname in os.listdir(IMAGES_DIR):
            if not fname.endswith('.png'):
                continue
            u = fname[:-4]
            if u not in current:
                os.remove(os.path.join(IMAGES_DIR, fname))
                manifest.pop(u, None)
                print(f"  - BRISEM (osirotela) {u[:8]}...")

    save_manifest(manifest)

    nfiles = len([f for f in os.listdir(IMAGES_DIR) if f.endswith('.png')])
    print()
    print("Koncano:")
    print(f"  + {new_cnt} novih")
    print(f"  ~ {changed_cnt} posodobljenih")
    print(f"  = {skip_cnt} nespremenjenih (preskoceno)")
    print(f"  x {fail_cnt} neuspesnih")
    print(f"  Skupaj v mapi: {nfiles} slik")


if __name__ == "__main__":
    main()
