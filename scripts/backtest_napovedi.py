#!/usr/bin/env python3
"""
backtest_napovedi.py — Koliko je model napovedi dejansko vreden?

Stran pri vsaki prihodnji tekmi izpiše verjetnost zmage, nikoli pa ne pove,
ali se je izšlo. Ta skripta to izmeri na odigranih sezonah.

POŠTENOST: tekme gremo kronološko in pred vsako napovedjo uporabimo SAMO
tekme, ki so bile odigrane pred njo. Ocene ob koncu sezone bi dale lažno
dobre rezultate (model bi "vedel" za izide, ki jih takrat ni mogel poznati).

KAJ MERIMO
  zadetost      delež tekem, kjer je zmagal favorit
  Brier         povprečje (napovedana verjetnost − izid)², nižje je bolje.
                0.25 = metanje kovanca, 0 = popolno
  kalibracija   ali se tekme, ki jim model da 70 %, res končajo 70:30

OMEJITEV: testiramo ELO del modela. Stran napoved zmeša 60 % ELO in 40 %
bElo (moč postave iz ocen igralcev); bElo je odvisen od sprotne statistike
igralcev in ga tu ne poustvarjamo. ELO je torej spodnja meja — tisto, kar
model zna samo iz izidov.

Uporaba:
  python scripts/backtest_napovedi.py
  python scripts/backtest_napovedi.py --sezona 26
  python scripts/backtest_napovedi.py --liga liga1
"""
import json
import os
import sys
from datetime import datetime

K = 32
START = 1500
# Prednost domacega igrisca v ELO tockah. Ni ugibana: iz 58,5 % domacih zmag
# sledi 60, in prav pri 60 je Brier na 383 tekmah najnizji. Ista vrednost je v
# aplikaciji (HOME_ELO).
HOME_PRIVZETO = 60
HOME = HOME_PRIVZETO


def arg(name, default=None):
    if name in sys.argv:
        i = sys.argv.index(name)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return default


def elo_p(r1, r2, h=0):
    return 1.0 / (1.0 + 10 ** ((r2 - (r1 + h)) / 400.0))


def nalozi(liga, sezona):
    """Vrne odigrane tekme rednega dela, urejene po času."""
    pot = f"data/{liga}_stats.json" if sezona in (None, 'tekoca') else f"data/{liga}_stats_s{sezona}.json"
    if not os.path.exists(pot):
        return None, pot
    d = json.load(open(pot, encoding='utf-8'))
    ms = []
    for m in d.get('allMatches', []):
        if m.get('status') != 'FINISHED':
            continue
        # Ista omejitev kot v computeElo(): ocene gradimo iz rednega dela.
        faza = ((m.get('competitions') or [{}])[0] or {}).get('competitionPhaseName')
        if faza != 'Redni del':
            continue
        if m.get('firstTeamScore') is None or m.get('secondTeamScore') is None:
            continue
        ms.append(m)
    ms.sort(key=lambda m: (m.get('dateTime') or '', m.get('id')))
    return ms, pot


def backtest(ms, min_tekem=3, home=None):
    """Kronološki sprehod: napovej, nato posodobi ocene."""
    r = {}
    odigranih = {}
    izidi = []          # (p_domaci, dejanski_izid 1/0)
    for m in ms:
        t1, t2 = m['firstTeamName'], m['secondTeamName']
        for t in (t1, t2):
            r.setdefault(t, START)
            odigranih.setdefault(t, 0)

        # Napoved SAMO iz dosedanjih tekem. Dokler ekipa nima nekaj tekem, je
        # njena ocena se vedno zacetnih 1500 in napoved ni nic vredna —
        # taksnih tekem ne stejemo, sicer bi si sami pokvarili meritev.
        if odigranih[t1] >= min_tekem and odigranih[t2] >= min_tekem:
            p1 = elo_p(r[t1], r[t2], HOME if home is None else home)
            zmagal1 = 1 if m['firstTeamScore'] > m['secondTeamScore'] else 0
            izidi.append((p1, zmagal1, m))

        # Posodobitev po tekmi (enako kot computeElo v aplikaciji). Brez
        # domacega igrisca: preizkuseno, z njim je Brier slabsi (0,1995 : 0,1991).
        e1 = elo_p(r[t1], r[t2])
        s1 = 1 if m['firstTeamScore'] > m['secondTeamScore'] else 0
        r[t1] = round(r[t1] + K * (s1 - e1))
        r[t2] = round(r[t2] + K * ((1 - s1) - (1 - e1)))
        odigranih[t1] += 1
        odigranih[t2] += 1
    return izidi, r


def porocaj(ime, izidi):
    if not izidi:
        print(f"  {ime}: premalo tekem za oceno")
        return None
    n = len(izidi)
    zadetih = sum(1 for p, w, _ in izidi if (p >= 0.5) == (w == 1))
    brier = sum((p - w) ** 2 for p, w, _ in izidi) / n
    domaci = sum(w for _, w, _ in izidi) / n
    # Kaj bi dosegel, ce bi vedno napovedal domaco zmago?
    dom_zadetost = domaci
    print(f"\n  {ime}  ({n} tekem)")
    print(f"    zadetost        {zadetih/n*100:5.1f} %")
    print(f"    Brier           {brier:.4f}   (0.25 = kovanec)")
    print(f"    delez dom. zmag {domaci*100:5.1f} %   -> 'vedno domaci' bi zadel {dom_zadetost*100:.1f} %")

    print(f"    kalibracija:")
    print(f"      {'napoved':>12s} {'tekem':>6s} {'dejansko':>9s}")
    for lo, hi in ((0.0,0.4),(0.4,0.5),(0.5,0.6),(0.6,0.7),(0.7,0.8),(0.8,1.01)):
        sk = [(p, w) for p, w, _ in izidi if lo <= p < hi]
        if not sk:
            continue
        pov = sum(p for p, _ in sk) / len(sk)
        dej = sum(w for _, w in sk) / len(sk)
        print(f"      {pov*100:10.0f} % {len(sk):6d} {dej*100:8.0f} %")
    return {'n': n, 'zadetost': zadetih/n, 'brier': brier, 'domaci': domaci}


def oceni(izidi):
    n = len(izidi)
    if not n:
        return None
    return {
        'n': n,
        'zadetost': sum(1 for p, w, _ in izidi if (p >= 0.5) == (w == 1)) / n,
        'brier': sum((p - w) ** 2 for p, w, _ in izidi) / n,
        'domaci': sum(w for _, w, _ in izidi) / n,
    }


def poisci_home(sezone, lige, min_tekem=150):
    """Poisce prednost domacega igrisca, ki da najnizji Brier.

    Vrednost ni fiksna: z vsako odigrano tekmo je vzorec vecji in ocena
    natancnejsa. Pod min_tekem se ne odlocamo — pri majhnem vzorcu bi
    optimizacija lovila sum in bi se stevilka divje premetavala iz tedna v
    teden. Takrat ostane privzeta.
    """
    skupaj = 0
    for sez in sezone:
        for lg in lige:
            ms, _ = nalozi(lg, sez)
            skupaj += len(ms or [])
    if skupaj < min_tekem:
        return HOME_PRIVZETO, None, skupaj

    najb = None
    for h in range(0, 161, 10):
        izidi = []
        for sez in sezone:
            for lg in lige:
                ms, _ = nalozi(lg, sez)
                if ms:
                    izidi += backtest(ms, home=h)[0]
        o = oceni(izidi)
        if o and (najb is None or o['brier'] < najb[1]['brier']):
            najb = (h, o)
    return najb[0], najb[1], skupaj


def main():
    global HOME
    lige = [arg('--liga')] if arg('--liga') else ['liga1', 'liga2', 'liga3']
    # Privzeto vzamemo VSE razpolozljive sezone — z vsako odigrano tekmo je
    # vzorec vecji in ocena natancnejsa. Model se tako ucи naprej sam.
    if arg('--sezona'):
        sezone = [None if arg('--sezona') in ('current', 'zdaj') else arg('--sezona')]
    else:
        sezone = [s for s in ('26',) if os.path.exists(f"data/liga1_stats_s{s}.json")] + [None]

    HOME, _o, n_vseh = poisci_home(sezone, lige)
    oznaka = ', '.join(str(x or 'tekoca') for x in sezone)

    print("=" * 62)
    print(f"Preveritev napovedi — sezone: {oznaka}")
    print("Napovedujemo samo iz tekem, odigranih PRED vsako tekmo.")
    print(f"Prednost domacega igrisca: {HOME} tock ELO "
          f"({'izmerjena na ' + str(n_vseh) + ' tekmah' if _o else 'privzeta, premalo tekem'})")
    print("=" * 62)

    vse = []
    for sez in sezone:
        for lg in lige:
            ms, pot = nalozi(lg, sez)
            if ms is None:
                continue
            if not ms:
                continue
            izidi, _ = backtest(ms)
            porocaj(f"{lg} · sezona {sez or 'tekoca'} ({len(ms)} odigranih)", izidi)
            vse += izidi

    if len(vse) > 1:
        print("\n" + "-" * 62)
        porocaj("SKUPAJ", vse)

    # Zapis za stran: da lahko pove, koliko je model vreden.
    if vse:
        n = len(vse)
        zad = sum(1 for p, w, _ in vse if (p >= 0.5) == (w == 1)) / n
        br = sum((p - w) ** 2 for p, w, _ in vse) / n
        dom = sum(w for _, w, _ in vse) / n
        with open('data/napovedi_tocnost.json', 'w', encoding='utf-8') as f:
            json.dump({
                'preverjeno': datetime.now().strftime('%Y-%m-%d'),
                'sezone': oznaka,
                'tekem': n,
                'zadetost': round(zad * 100, 1),
                'brier': round(br, 4),
                'domacihZmag': round(dom * 100, 1),
                'homeElo': HOME,
            }, f, ensure_ascii=False, indent=1)
        print(f"\n  ✅ data/napovedi_tocnost.json")

    # Najbolj presenetljive tekme — koristno za zdravo pamet.
    if vse:
        presenecenja = sorted(vse, key=lambda x: abs(x[0] - x[1]), reverse=True)[:5]
        print("\n  Najvecja presenecenja:")
        for p, w, m in presenecenja:
            izid = f"{m['firstTeamScore']}:{m['secondTeamScore']}"
            print(f"    {m['firstTeamName']} {izid} {m['secondTeamName']}"
                  f"  — napoved {p*100:.0f} % za domace, {'zmagali' if w else 'izgubili'}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
