#!/usr/bin/env python3
"""
fetch_meti.py — Lokacije metov iz FIBA LiveStats -> data/{liga}_meti.json

Karta metov na strani. FIBA ima pri vsaki tekmi 1. in 2. SKL seznam metov z
lokacijo; 3. SKL ni na FIBA. Datoteka se naloži šele, ko jo obiskovalec odpre.

KOORDINATE: FIBA podaja x in y v odstotkih celega igrišča (28 x 15 m), koš je
pri x = 5,6 % oziroma 94,4 %. Ekipi po polčasu zamenjata stran, zato mete z
desne polovice prezrcalimo: vsi meti so na isti polovici, x je od osnovne črte.
Preverjeno: tako so vse trojke od 6,8 m naprej (črta je 6,75 m), dvojke do 6,3 m.

IZHOD: {"seasonId": 27, "tekme": {"109818": [["Krka", [[pid,x,y,zadet,cetrtina,tocke,vrsta], ...]], [...]]}}
  x, y   desetine odstotka igrišča (x 0..500 od osnovne črte, y 0..1000)
  vrsta  indeks v VRSTE (met iz skoka, polaganje ...)

PREVERJANJE: po tekmi preštejemo mete vsakega igralca in jih primerjamo z
zapisnikom KZS. Tekma, kjer se ne ujema, se izpiše, a ostane: nekaj metov je
lahko označenih drugače, karta pa je še vedno pravilna.

Uporaba:  python scripts/fetch_meti.py               (tekoča sezona, samo nove tekme)
          python scripts/fetch_meti.py --sezona 26   (arhiv -> data/{liga}_meti_s26.json)
"""
import json
import os
import re
import sys
import time
import urllib.request
from collections import Counter

FIBA = "https://fibalivestats.dcd.shared.geniussports.com/data"
LIGE = ('liga1', 'liga2')          # 3. SKL ni na FIBA LiveStats
VRSTE = ['jumpshot', 'layup', 'dunk', 'hookshot', 'other']


def arg(ime):
    return sys.argv[sys.argv.index(ime) + 1] if ime in sys.argv[:-1] else None


def fetch_json(url, poskusov=3):
    for i in range(poskusov):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode())
        except Exception:
            time.sleep(i + 1)
    return None


def norm(s):
    s = (s or '').lower()
    for a, b in (('č', 'c'), ('ć', 'c'), ('š', 's'), ('ž', 'z'), ('đ', 'd')):
        s = s.replace(a, b)
    return re.sub(r'[^a-z]', '', s)


def vrsta(sub):
    sub = (sub or '').lower()
    for i, v in enumerate(VRSTE[:-1]):
        if v in sub:
            return i
    return len(VRSTE) - 1


def poveži(pl, ekipa):
    """FIBA igralec -> playerId KZS. Polno ime, sicer priimek in začetnica, če je enolično."""
    kzs = {(norm(p.get('matchTeamPlayerLastname')), norm(p.get('matchTeamPlayerFirstname'))): p['playerId']
           for p in (ekipa or {}).get('playerStats') or []}
    out = {}
    for k, f in pl.items():
        pr, ime = norm(f.get('familyName')), norm(f.get('firstName'))
        pid = kzs.get((pr, ime))
        if not pid:
            kand = [v for (a, b), v in kzs.items() if a == pr and b[:1] == ime[:1]]
            pid = kand[0] if len(kand) == 1 else None
        if pid:
            out[k] = pid
    return out


def obdelaj(d, md):
    """FIBA data.json -> [[ekipa, [met, ...]], ...] in število metov brez igralca."""
    tm = d.get('tm') or {}
    strani = (md.get('firstTeam'), md.get('secondTeam'))
    izid, neznani = [], 0
    for tno in ('1', '2'):
        t = tm.get(tno) or {}
        pl = t.get('pl') or {}
        # FIBA ekipa 1 ni nujno KZS prva: vzamemo stran z več ujemajočimi imeni.
        povezave = [poveži(pl, s) for s in strani]
        i = 0 if len(povezave[0]) >= len(povezave[1]) else 1
        pidi = povezave[i]
        meti = []
        for s in t.get('shot') or []:
            x, y = s.get('x') or 0, s.get('y') or 0
            if x > 50:
                x, y = 100 - x, 100 - y
            pid = pidi.get(str(s.get('p')))
            neznani += pid is None
            per = s.get('per') or 0
            if s.get('perType') == 'OVERTIME':
                per += 4
            meti.append([pid or 0, round(x * 10), round(y * 10), 1 if s.get('r') else 0, per,
                         3 if s.get('actionType') == '3pt' else 2, vrsta(s.get('subType'))])
        izid.append([(strani[i] or {}).get('teamName') or t.get('name'), meti])
    return izid, neznani


def preveri(izid, md):
    """Primerja število metov z zapisnikom; vrne število igralcev, ki se ne ujemajo."""
    šteto = Counter()
    for _, meti in izid:
        for m in meti:
            šteto[(m[0], m[5], 'a')] += 1
            šteto[(m[0], m[5], 'm')] += m[3]
    slabo = 0
    for stran in ('firstTeam', 'secondTeam'):
        for p in (md.get(stran) or {}).get('playerStats') or []:
            pid = p['playerId']
            if (šteto[(pid, 2, 'a')], šteto[(pid, 2, 'm')], šteto[(pid, 3, 'a')], šteto[(pid, 3, 'm')]) != (
                    p.get('twoPA') or 0, p.get('twoPM') or 0, p.get('threePA') or 0, p.get('threePM') or 0):
                slabo += 1
    return slabo


def main():
    sezona = arg('--sezona')
    pri = f'_s{sezona}' if sezona else ''
    for lg in LIGE:
        pot = f'data/{lg}_stats{pri}.json'
        if not os.path.exists(pot):
            continue
        d = json.load(open(pot, encoding='utf-8'))
        izhod = f'data/{lg}_meti{pri}.json'
        staro = {}
        if os.path.exists(izhod):
            s = json.load(open(izhod, encoding='utf-8'))
            if s.get('seasonId') == d.get('seasonId'):
                staro = s.get('tekme') or {}
        ms = d.get('matchStats') or {}
        potrebne = [m for m in d.get('allMatches', [])
                    if m.get('status') == 'FINISHED' and m.get('fibaLiveStatsUrl')
                    and str(m['id']) not in staro and ms.get(str(m['id']))]
        print(f"  {lg}: {len(staro)} shranjenih, {len(potrebne)} novih")
        slabih = brez = 0
        for m in potrebne:
            fid = m['fibaLiveStatsUrl'].rstrip('/').split('/')[-1]
            f = fetch_json(f"{FIBA}/{fid}/data.json")
            time.sleep(0.3)
            if not f or not any(((f.get('tm') or {}).get(t) or {}).get('shot') for t in ('1', '2')):
                continue
            md = ms[str(m['id'])]
            izid, neznani = obdelaj(f, md)
            brez += neznani
            r = preveri(izid, md)
            if r:
                slabih += 1
                print(f"    ! {m['id']} {m['firstTeamName']} : {m['secondTeamName']}: {r} igralcev se ne ujema z zapisnikom")
            staro[str(m['id'])] = izid
        with open(izhod, 'w', encoding='utf-8') as f:
            json.dump({'seasonId': d.get('seasonId'), 'vrste': VRSTE, 'tekme': staro},
                      f, ensure_ascii=False, separators=(',', ':'))
        print(f"  ✅ {izhod} ({os.path.getsize(izhod) // 1024} KB), tekem {len(staro)}, "
              f"neujemanj {slabih}, metov brez igralca {brez}")


if __name__ == '__main__':
    main()
