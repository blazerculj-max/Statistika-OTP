#!/usr/bin/env python3
"""
peterke.py — Peterke in vpliv igralca iz play-by-play -> data/{liga}_peterke.json

Iz začetne peterke in menjav sledimo, kdo je bil na igrišču, in vsaki peterki
pripišemo čas, točke in posesti. Tako dobimo:
  - najboljše peterke vsake ekipe,
  - učinek ekipe z igralcem na igrišču in brez njega.

POSESTI: meti iz igre + izgube + 0,44 x prosti meti - napadalni skoki, šteti po
dogodkih, zato peterki pripadajo posesti, ki jih je res odigrala. Točke na 100
posesti so primerljive tudi med peterkami z različno minutažo.

PREVERJANJE: minute in plus-minus vsakega igralca morata biti enaka kot v zapisniku
KZS. Skript izpiše odstopanja; to je dokaz, da je sledenje peterki pravilno.

3. SKL ni na FIBA. Arhiv 2025/26 nima menjav, zato samo tekoča sezona.

Uporaba:  python scripts/peterke.py
"""
import json
import os
import re
import sys
from collections import Counter, defaultdict

LIGE = ('liga1', 'liga2')
NAJVEC_PETERK = 12        # na ekipo
NAJMANJ_SEKUND = 120      # krajše peterke niso vredne vrstice


def norm(s):
    s = (s or '').lower()
    for a, b in (('č', 'c'), ('ć', 'c'), ('š', 's'), ('ž', 'z'), ('đ', 'd')):
        s = s.replace(a, b)
    return re.sub(r'[^a-z]', '', s)


def sekunde(gt):
    try:
        mm, ss = str(gt).split(':')[:2]
        return int(mm) * 60 + int(float(ss))
    except Exception:
        return 0


def perioda(e):
    p = int(e.get('period') or 1)
    return p + 4 if e.get('periodType') == 'OVERTIME' else p


def cas(e):
    """Sekunde od začetka tekme."""
    p = perioda(e)
    if p <= 4:
        return (p - 1) * 600 + 600 - sekunde(e.get('gt'))
    return 2400 + (p - 5) * 300 + 300 - sekunde(e.get('gt'))


def konec_periode(p):
    return p * 600 if p <= 4 else 2400 + (p - 4) * 300


def uredi(ev):
    """
    Popravi vrstni red menjav. FIBA v seznamu menjavo pogosto postavi pred proste
    mete, čeprav je pri njej že zapisan rezultat PO njih (menjava 57:41 pred metoma
    57:40 in 57:41). Taka menjava se je zgodila za meti, zato jo prestavimo.
    """
    def skupaj(e):
        try:
            return int(e.get('s1')) + int(e.get('s2'))
        except (TypeError, ValueError):
            return None

    izid, cakajo, tocke = [], [], 0
    for e in ev:
        if cakajo and cas(e) != cas(cakajo[0]):
            izid += cakajo
            cakajo = []
        s = skupaj(e)
        if e.get('actionType') == 'substitution' and s is not None and s > tocke:
            cakajo.append(e)
            continue
        izid.append(e)
        if s is not None:
            tocke = max(tocke, s)
        if cakajo and tocke >= skupaj(cakajo[0]):
            izid += cakajo
            cakajo = []
    return izid + cakajo


def igralci_tekme(md):
    """Vsaka stran: (priimek, ime) -> playerId."""
    out = []
    for k in ('firstTeam', 'secondTeam'):
        t = md.get(k) or {}
        out.append((t.get('teamName'), {(norm(p.get('matchTeamPlayerLastname')), norm(p.get('matchTeamPlayerFirstname'))): p['playerId']
                                        for p in t.get('playerStats') or []}))
    return out


def najdi(imena, e):
    pr, ime = norm(e.get('familyName')), norm(e.get('firstName'))
    pid = imena.get((pr, ime))
    if pid:
        return pid
    kand = [v for (a, b), v in imena.items() if a == pr and b[:1] == ime[:1]]
    return kand[0] if len(kand) == 1 else None


def obdelaj_tekmo(ev, md, zbir, kontrola):
    ev = uredi(list(reversed(ev)))      # shranjeno od najnovejšega naprej
    strani = igralci_tekme(md)
    # FIBA ekipa 1 ni nujno KZS prva: odloči večina imen
    glasovi = Counter()
    for e in ev:
        if e.get('tno') in (1, 2) and e.get('familyName'):
            for i, (_, imena) in enumerate(strani):
                if najdi(imena, e):
                    glasovi[(e['tno'], i)] += 1
    obrnjeno = glasovi[(1, 1)] > glasovi[(1, 0)]
    ekipa = {1: strani[1 if obrnjeno else 0], 2: strani[0 if obrnjeno else 1]}

    na = {1: set(), 2: set()}
    sek = {1: Counter(), 2: Counter()}
    pm = {1: Counter(), 2: Counter()}
    prej_t, prej_s = 0, (0, 0)

    def vnos(tno):
        return zbir[ekipa[tno][0]]

    def peterka(tno):
        return tuple(sorted(na[tno])) if len(na[tno]) == 5 else None

    def dodaj(tno, polje, koliko):
        z = vnos(tno)
        k = peterka(tno)
        if k:
            z['peterke'][k][polje] += koliko
            z['skupaj'][polje] += koliko
            for p in k:
                z['igralci'][p][polje] += koliko

    def teci(do):
        nonlocal prej_t
        dt = do - prej_t
        if dt <= 0:
            return
        for tno in (1, 2):
            for p in na[tno]:
                sek[tno][p] += dt
            dodaj(tno, 'sek', dt)
            if len(na[tno]) != 5:
                kontrola['brez_peterke'] += dt
            kontrola['skupaj_sek'] += dt
        prej_t = do

    def posest(tno, koliko):
        dodaj(tno, 'pz', koliko)
        dodaj(3 - tno, 'pp', koliko)

    zadnja = 1
    for e in ev:
        a = e.get('actionType')
        tno = e.get('tno')
        zadnja = max(zadnja, perioda(e))
        teci(cas(e))
        if a == 'starter':
            pid = najdi(ekipa[tno][1], e)
            if pid:
                na[tno].add(pid)
            continue
        if a == 'substitution':
            pid = najdi(ekipa[tno][1], e)
            if pid:
                (na[tno].add if e.get('subType') == 'in' else na[tno].discard)(pid)
            continue
        # rezultat: FIBA ga pripne vsakemu dogodku
        s1, s2 = e.get('s1'), e.get('s2')
        if s1 not in ('', None) and s2 not in ('', None):
            nov = (int(s1), int(s2))
            d1, d2 = nov[0] - prej_s[0], nov[1] - prej_s[1]
            if d1 or d2:
                for t, za, proti in ((1, d1, d2), (2, d2, d1)):
                    dodaj(t, 'za', za)
                    dodaj(t, 'proti', proti)
                    for p in na[t]:
                        pm[t][p] += za - proti
                prej_s = nov
        if tno in (1, 2):
            if a in ('2pt', '3pt'):
                posest(tno, 1)
            elif a == 'turnover':
                posest(tno, 1)
            elif a == 'freethrow':
                posest(tno, .44)
            elif a == 'rebound' and e.get('subType') == 'offensive':
                posest(tno, -1)
    teci(konec_periode(zadnja))

    # preverjanje z zapisnikom
    for tno in (1, 2):
        i = (1 if obrnjeno else 0) if tno == 1 else (0 if obrnjeno else 1)
        for p in (md.get(('firstTeam', 'secondTeam')[i]) or {}).get('playerStats') or []:
            if not p.get('played'):
                continue
            pid = p['playerId']
            kontrola['igralcev'] += 1
            if abs(sek[tno][pid] / 60 - (p.get('minutes') or 0)) > 0.5:
                kontrola['minute_odstopajo'] += 1
            if pm[tno][pid] != (p.get('plusMinus') or 0):
                kontrola['pm_odstopa'] += 1
                if len(kontrola['primeri']) < 6:
                    kontrola['primeri'].append(f"{md.get('matchId')} {p.get('matchTeamPlayerLastname')}: "
                                               f"+/- {pm[tno][pid]} / {p.get('plusMinus')}, min {sek[tno][pid] / 60:.1f} / {p.get('minutes')}")
    for tno in (1, 2):
        vnos(tno)['tekme'] += 1


def zaokrozi(c):
    return {'min': round(c['sek'] / 60, 1), 'za': c['za'], 'proti': c['proti'],
            'pz': round(c['pz'], 1), 'pp': round(c['pp'], 1)}


def main():
    for lg in LIGE:
        pot_s, pot_p = f'data/{lg}_stats.json', f'data/{lg}_pbp.json'
        if not (os.path.exists(pot_s) and os.path.exists(pot_p)):
            continue
        d = json.load(open(pot_s, encoding='utf-8'))
        pbp = json.load(open(pot_p, encoding='utf-8')).get('pbp') or {}
        ms = d.get('matchStats') or {}
        zbir = defaultdict(lambda: {'tekme': 0, 'skupaj': Counter(), 'peterke': defaultdict(Counter), 'igralci': defaultdict(Counter)})
        kontrola = Counter()
        kontrola['primeri'] = []
        n = 0
        for m in d.get('allMatches', []):
            mid = str(m['id'])
            ev, md = pbp.get(mid), ms.get(mid)
            if m.get('status') != 'FINISHED' or not ev or not md:
                continue
            if not any(e.get('actionType') == 'starter' for e in ev):
                continue
            obdelaj_tekmo(ev, md, zbir, kontrola)
            n += 1
        ekipe = {}
        for ime, z in zbir.items():
            pet = sorted(z['peterke'].items(), key=lambda kv: -kv[1]['sek'])
            ekipe[ime] = {
                'tekme': z['tekme'], 'skupaj': zaokrozi(z['skupaj']),
                'peterke': [dict(zaokrozi(c), p=list(k)) for k, c in pet[:NAJVEC_PETERK] if c['sek'] >= NAJMANJ_SEKUND],
                'igralci': {str(p): zaokrozi(c) for p, c in z['igralci'].items()},
            }
        izhod = f'data/{lg}_peterke.json'
        with open(izhod, 'w', encoding='utf-8') as f:
            json.dump({'seasonId': d.get('seasonId'), 'tekem': n, 'ekipe': ekipe}, f, ensure_ascii=False, separators=(',', ':'))
        delez = kontrola['brez_peterke'] / max(kontrola['skupaj_sek'], 1) * 100
        print(f"  {lg}: {n} tekem, {kontrola['igralcev']} nastopov; minute odstopajo pri {kontrola['minute_odstopajo']}, "
              f"plus-minus pri {kontrola['pm_odstopa']}; brez popolne peterke {delez:.2f} % časa ({os.path.getsize(izhod) // 1024} KB)")
        for p in kontrola['primeri']:
            print('     ', p)


if __name__ == '__main__':
    main()
