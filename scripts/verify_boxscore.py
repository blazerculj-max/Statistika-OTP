#!/usr/bin/env python3
"""
verify_boxscore.py — Primerja uradni zapisnik KZS s FIBA LiveStats.

ZAKAJ: stran po koncu tekme takoj pokaže boxscore iz FIBA, ker uradni zapisnik
KZS pride šele kasneje. Ko ta pride, je prav, da preverimo, ali se ujema —
sicer bi lahko obiskovalcu nekaj časa kazali napačne številke, pa tega nikoli
ne bi izvedeli.

KAJ PRIMERJA (na igralca, po imenu):
  točke, zadete mete za 2 in 3, zadete proste mete, skupne skoke, asistence
Ekipno pa še končni rezultat.

Imena se med sistemoma pišejo različno (šumniki, vrstni red, drugo ime), zato
jih normaliziramo in primerjamo po priimku + začetnici imena — isti pristop kot
v aplikaciji.

Uporaba:
  python scripts/verify_boxscore.py              # vse odigrane tekme te sezone
  python scripts/verify_boxscore.py --liga liga1
  python scripts/verify_boxscore.py --match 109822
  python scripts/verify_boxscore.py --json       # strojno berljiv izpis
"""
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error

PROXY = "https://floral-bush-24fe.blazerculj.workers.dev/?id="
LIGE = ('liga1', 'liga2', 'liga3')
# Dovoljeno odstopanje: 0 = zahtevamo popolno ujemanje.
TOLERANCA = 0


def arg(name, default=None):
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def norm(s):
    s = (s or '').lower()
    for a, b in (('č','c'),('ć','c'),('š','s'),('ž','z'),('đ','d')):
        s = s.replace(a, b)
    return re.sub(r'[^a-z]', '', s)


def kljuc(ime, priimek):
    """Priimek + začetnica imena — dovolj za razlikovanje znotraj ene ekipe."""
    p = norm(priimek)
    i = norm(ime)
    return f"{p}|{i[:1]}"


def fetch_fiba(fid, retries=2):
    for n in range(retries):
        try:
            req = urllib.request.Request(PROXY + str(fid),
                                         headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 403:
                return None          # tekme ni na FIBA (npr. 3. SKL)
            time.sleep(n + 1)
        except Exception:
            time.sleep(n + 1)
    return None


def iz_fibe(d):
    """FIBA -> {ekipa_index: {kljuc: {metrika: vrednost}}} + rezultat."""
    out, score = {}, {}
    for ti, key in enumerate(('1', '2')):
        t = (d.get('tm') or {}).get(key) or {}
        score[ti] = t.get('score')
        igralci = {}
        for p in (t.get('pl') or {}).values():
            igralci[kljuc(p.get('firstName'), p.get('familyName'))] = {
                'toc': p.get('sPoints') or 0,
                '2m':  p.get('sTwoPointersMade') or 0,
                '3m':  p.get('sThreePointersMade') or 0,
                'pm':  p.get('sFreeThrowsMade') or 0,
                'sk':  p.get('sReboundsTotal') or 0,
                'as':  p.get('sAssists') or 0,
            }
        out[ti] = igralci
    return out, score


def iz_kzs(md):
    out, score = {}, {}
    for ti, key in enumerate(('firstTeam', 'secondTeam')):
        t = md.get(key) or {}
        igralci = {}
        skupaj = 0
        for p in (t.get('playerStats') or []):
            k = kljuc(p.get('matchTeamPlayerFirstname'), p.get('matchTeamPlayerLastname'))
            igralci[k] = {
                'toc': p.get('points') or 0,
                '2m':  p.get('twoPM') or 0,
                '3m':  p.get('threePM') or 0,
                'pm':  p.get('fTM') or 0,
                'sk':  (p.get('offensiveRebounds') or 0) + (p.get('defensiveRebounds') or 0),
                'as':  p.get('assists') or 0,
            }
            skupaj += igralci[k]['toc']
        out[ti] = igralci
        score[ti] = skupaj
    return out, score


def preveri_tekmo(m, md):
    """Vrne seznam odstopanj za eno tekmo (prazen = vse se ujema)."""
    fid = (m.get('fibaLiveStatsUrl') or '').rstrip('/').split('/')[-1]
    if not fid.isdigit():
        return None, 'ni FIBA povezave'
    d = fetch_fiba(fid)
    if not d:
        return None, 'FIBA nima te tekme'

    f_pl, f_sc = iz_fibe(d)
    k_pl, k_sc = iz_kzs(md)
    napake = []

    for ti in (0, 1):
        ime_ekipe = (md.get('firstTeam' if ti == 0 else 'secondTeam') or {}).get('teamName', f'ekipa {ti+1}')

        # Končni rezultat
        uradni = m.get('firstTeamScore' if ti == 0 else 'secondTeamScore')
        if f_sc.get(ti) is not None and uradni is not None and f_sc[ti] != uradni:
            napake.append(f"{ime_ekipe}: rezultat KZS {uradni} vs FIBA {f_sc[ti]}")

        # Igralci, ki jih ima en sistem, drugi pa ne
        samo_kzs  = set(k_pl[ti]) - set(f_pl[ti])
        samo_fiba = set(f_pl[ti]) - set(k_pl[ti])
        # Igralca brez učinka (0 točk in 0 vsega) ne štejemo za odstopanje —
        # sistema se razlikujeta pri tem, koga sploh zapišeta na klop.
        for k in sorted(samo_kzs):
            if any(k_pl[ti][k].values()):
                napake.append(f"{ime_ekipe}: {k} je v KZS, v FIBA ga ni")
        for k in sorted(samo_fiba):
            if any(f_pl[ti][k].values()):
                napake.append(f"{ime_ekipe}: {k} je v FIBA, v KZS ga ni")

        # Metrike pri skupnih igralcih
        for k in sorted(set(k_pl[ti]) & set(f_pl[ti])):
            for met in ('toc', '2m', '3m', 'pm', 'sk', 'as'):
                a, b = k_pl[ti][k][met], f_pl[ti][k][met]
                if abs(a - b) > TOLERANCA:
                    napake.append(f"{ime_ekipe}: {k} {met} — KZS {a}, FIBA {b}")
    return napake, None


def main():
    lige = [arg('--liga')] if arg('--liga') else list(LIGE)
    samo = arg('--match')
    kot_json = '--json' in sys.argv
    izid = {'preverjenih': 0, 'ujema': 0, 'odstopa': 0, 'preskoceno': 0, 'tekme': []}

    for lg in lige:
        path = f"data/{lg}_stats.json"
        if not os.path.exists(path):
            continue
        d = json.load(open(path, encoding='utf-8'))
        st = d.get('matchStats') or {}
        for m in d.get('allMatches', []):
            if m.get('status') != 'FINISHED':
                continue
            if samo and str(m['id']) != str(samo):
                continue
            md = st.get(str(m['id']))
            if not md:
                continue
            napake, razlog = preveri_tekmo(m, md)
            opis = (f"{m.get('firstTeamName')} {m.get('firstTeamScore')}:"
                    f"{m.get('secondTeamScore')} {m.get('secondTeamName')}")
            if razlog:
                izid['preskoceno'] += 1
                if not kot_json:
                    print(f"  – {lg} {m['id']}  {opis}  ({razlog})")
                continue
            izid['preverjenih'] += 1
            if napake:
                izid['odstopa'] += 1
                izid['tekme'].append({'liga': lg, 'id': m['id'], 'opis': opis, 'napake': napake})
                if not kot_json:
                    print(f"  ✗ {lg} {m['id']}  {opis}")
                    for n in napake[:12]:
                        print(f"        {n}")
                    if len(napake) > 12:
                        print(f"        … in še {len(napake)-12}")
            else:
                izid['ujema'] += 1
                if not kot_json:
                    print(f"  ✓ {lg} {m['id']}  {opis}")

    if kot_json:
        print(json.dumps(izid, ensure_ascii=False, indent=2))
    else:
        print()
        print(f"Preverjenih: {izid['preverjenih']}  |  ujema se: {izid['ujema']}  "
              f"|  odstopa: {izid['odstopa']}  |  preskočeno: {izid['preskoceno']}")
    # Odstopanje ni razlog za padec zagona — je opozorilo, ne okvara.
    return 0


if __name__ == '__main__':
    sys.exit(main())
