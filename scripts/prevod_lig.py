#!/usr/bin/env python3
"""
prevod_lig.py — Koliko je 20 točk v 3. SKL vrednih v 1. SKL?

METODA: igralci, ki so med sezonama zamenjali ligo. Če isti igralec v 2. SKL
doseže 15 točk na tekmo, naslednjo sezono pa v 1. SKL 9, je razmerje 0,60.
Mediana čez vse take prehode je ocena prevodnega faktorja.

ZAKAJ MED SEZONAMA IN NE ZNOTRAJ: preverjeno 9.10.2026 je 111 igralcev
nastopilo v več kot eni ligi v isti sezoni, a so to skoraj brez izjeme
klicanja — mladi igralec odigra 25 tekem v 3. SKL in dve na klopi v 1. SKL.
Razmerje takrat meri vlogo in minute, ne težavnosti lige. Primer: Erolt
Rrahmani 4,1 točke na tekmo v 1. SKL (22 tekem s klopi) proti 20,5 v 3. SKL
(14 tekem kot prva opcija). Naivno razmerje bi reklo, da je 1. SKL petkrat
težja, kar je nesmisel.

Prehod med sezonama je čistejši: igralec v obeh ligah igra svojo pravo vlogo.

STANJE: metoda je pripravljena, podatkov pa še ni dovolj. 100 igralcev je
zamenjalo ligo med 2025/26 in 2026/27, a so letos odigrali komaj nekaj tekem.
Skripta zato sama pove, koliko vzorca še manjka, in zapiše oceno šele, ko je
dovolj. Sredi sezone se bo vklopila sama.

Uporaba:
  python scripts/prevod_lig.py
  python scripts/prevod_lig.py --min-tekem 8
"""
import json
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime

LIGE = ('liga1', 'liga2', 'liga3')
IMENA = {'liga1': '1. SKL', 'liga2': '2. SKL', 'liga3': '3. SKL'}
# Koliko tekem mora igralec odigrati v VSAKI sezoni, da ga štejemo.
MIN_TEKEM = 8
# Koliko igralcev mora podpirati en par lig, da oceni sploh zaupamo.
MIN_IGRALCEV = 5


def arg(name, default=None):
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def sezone_na_voljo():
    """[('26','_s26'), ('27','')] — arhivi plus tekoča."""
    out = []
    for s in ('24', '25', '26'):
        if os.path.exists(f"data/liga1_stats_s{s}.json"):
            out.append((s, f"_s{s}"))
    out.append(('tekoca', ''))
    return out


def naberi(suf):
    """pid -> liga -> [tekme, točke]; liga je tista z največ nastopi."""
    agg = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    imena = {}
    for lg in LIGE:
        pot = f"data/{lg}_stats{suf}.json"
        if not os.path.exists(pot):
            continue
        d = json.load(open(pot, encoding='utf-8'))
        for _mid, md in (d.get('matchStats') or {}).items():
            if not md:
                continue
            for stran in ('firstTeam', 'secondTeam'):
                for p in ((md.get(stran) or {}).get('playerStats') or []):
                    pid = p.get('playerId')
                    if not pid or not p.get('played'):
                        continue
                    a = agg[pid][lg]
                    a[0] += 1
                    a[1] += p.get('points') or 0
                    imena[pid] = (f"{p.get('matchTeamPlayerFirstname','')} "
                                  f"{p.get('matchTeamPlayerLastname','')}").strip()
    # obdrzi samo glavno ligo igralca v tej sezoni
    out = {}
    for pid, po_ligah in agg.items():
        lg = max(po_ligah, key=lambda k: po_ligah[k][0])
        t, toc = po_ligah[lg]
        out[pid] = (lg, t, toc)
    return out, imena


def main():
    min_tekem = int(arg('--min-tekem', MIN_TEKEM))
    sez = sezone_na_voljo()
    print("=" * 64)
    print("Prevod med ligami — iz igralcev, ki so zamenjali ligo med sezonama")
    print(f"Prag: vsaj {min_tekem} tekem v vsaki od obeh sezon")
    print("=" * 64)

    podatki, imena = {}, {}
    for oznaka, suf in sez:
        podatki[oznaka], im = naberi(suf)
        imena.update(im)
        n = len(podatki[oznaka])
        print(f"  sezona {oznaka:7s}: {n} igralcev")

    pari = defaultdict(list)       # (visja, nizja) -> [razmerje]
    primeri = defaultdict(list)
    premalo = 0
    for i in range(len(sez) - 1):
        a_o, b_o = sez[i][0], sez[i + 1][0]
        A, B = podatki[a_o], podatki[b_o]
        for pid in set(A) & set(B):
            (lg_a, t_a, toc_a) = A[pid]
            (lg_b, t_b, toc_b) = B[pid]
            if lg_a == lg_b:
                continue
            if t_a < min_tekem or t_b < min_tekem:
                premalo += 1
                continue
            p_a, p_b = toc_a / t_a, toc_b / t_b
            if p_a <= 0 or p_b <= 0:
                continue
            # Vedno v smeri "visja liga / nizja liga" (liga1 < liga2 < liga3 po imenu)
            if lg_a < lg_b:
                visja, nizja, r = lg_a, lg_b, p_a / p_b
            else:
                visja, nizja, r = lg_b, lg_a, p_b / p_a
            pari[(visja, nizja)].append(r)
            primeri[(visja, nizja)].append(
                (imena.get(pid, '?'), lg_a, t_a, p_a, lg_b, t_b, p_b, r))

    print(f"\n  Prehodov pod pragom (premalo tekem): {premalo}")
    izid = {'izracunano': datetime.now().strftime('%Y-%m-%d'),
            'minTekem': min_tekem, 'pari': {}}
    nic = True
    for (visja, nizja), rs in sorted(pari.items()):
        kljuc = f"{visja}/{nizja}"
        if len(rs) < MIN_IGRALCEV:
            print(f"\n  {IMENA[visja]} ← {IMENA[nizja]}: samo {len(rs)} igralcev "
                  f"(potrebnih {MIN_IGRALCEV}) — ocene ne objavimo")
            continue
        nic = False
        med = statistics.median(rs)
        print(f"\n  {IMENA[visja]} ← {IMENA[nizja]}  (n={len(rs)})")
        print(f"    faktor {med:.2f}  — 10 točk v {IMENA[nizja]} ≈ "
              f"{10*med:.1f} točke v {IMENA[visja]}")
        print(f"    razpon {min(rs):.2f}–{max(rs):.2f}")
        for n_, la, ta, pa, lb, tb, pb, r in sorted(primeri[(visja, nizja)],
                                                    key=lambda x: -x[2])[:5]:
            print(f"      {n_[:22]:23s} {IMENA[la]} {ta:2d}tm {pa:4.1f}  →  "
                  f"{IMENA[lb]} {tb:2d}tm {pb:4.1f}   ({r:.2f})")
        izid['pari'][kljuc] = {'faktor': round(med, 3), 'igralcev': len(rs),
                               'min': round(min(rs), 2), 'max': round(max(rs), 2)}

    if nic:
        print("\n  Za nobeno kombinacijo lig še ni dovolj igralcev.")
        print("  Metoda deluje, manjka samo odigranih tekem tekoče sezone.")
        print("  Ocena se bo pojavila sama, ko bo vzorec dovolj velik.")
    else:
        with open('data/prevod_lig.json', 'w', encoding='utf-8') as f:
            json.dump(izid, f, ensure_ascii=False, indent=1)
        print("\n  ✅ data/prevod_lig.json")
    return 0


if __name__ == '__main__':
    sys.exit(main())
