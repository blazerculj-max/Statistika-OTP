#!/usr/bin/env python3
"""
nove_prijave.py — Kdo je bil med sezono na novo prijavljen (data/prijave.json).

ZAKAJ: KZS objavi samo trenutni sestav, brez datuma prijave. Prestopi zato
primerjajo le lansko in letosnjo sezono — igralec, ki ga klub prijavi sredi
sezone, je tam med prihodi, a ne vemo, kdaj je prisel. Datum dobimo tako, da
si pri vsakem zagonu zapomnimo, kdaj smo igralca v sestavu videli prvic.

KAJ STEJE ZA NOVO PRIJAVO:
  - igralec se je v sestavu ekipe pojavil PO 1. krogu lige (prej so to
    obicajne predsezonske prijave — te pokrijejo Prestopi), in
  - ni del prve objave sestava te ekipe (klub, ki je sestav objavil sele
    po zacetku sezone, sicer ne bi imel 12 "novih" igralcev naenkrat).
Igralec, ki iz sestava izgine, dobi datum odjave; ce se vrne, ga izgubi.

Natancnost datuma je en zagon delovnega toka (6 ur).

Uporaba:  python scripts/nove_prijave.py               (po fetch_rosters.py)
          python scripts/nove_prijave.py --zgodovina   (enkrat: iz git zgodovine)
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

DATA = 'data'
POT = os.path.join(DATA, 'prijave.json')
LIGE = ('liga1', 'liga2', 'liga3')


def prazno(sid):
    return {'seasonId': sid, 'ekipe': {}, 'igralci': {}, 'odjave': {}}


def nalozi_stanje(sid):
    try:
        st = json.load(open(POT, encoding='utf-8'))
    except Exception:
        return prazno(sid)
    if st.get('seasonId') != sid:
        # Nova sezona: lansko stanje ostane pod pripono, zacnemo znova.
        os.replace(POT, os.path.join(DATA, f"prijave_s{st.get('seasonId')}.json"))
        print(f"  nova sezona {sid}: prejsnje stanje je zdaj prijave_s{st.get('seasonId')}.json")
        return prazno(sid)
    return st


def vnesi(st, rost, cas):
    """En posnetek sestavov: zapomni si prvic videne igralce in odjave."""
    for lg in LIGE:
        L = (rost.get('leagues') or {}).get(lg)
        if not L:
            continue
        ek, ig, od = (st[k].setdefault(lg, {}) for k in ('ekipe', 'igralci', 'odjave'))
        for t in L.get('teams', []):
            pidi = {str(p['pid']) for p in t.get('players', []) if p.get('pid')}
            tid = str(t['id'])
            # Prazen sestav je lahko tudi napaka API-ja — iz njega ne sklepamo nicesar.
            if not pidi:
                continue
            ek.setdefault(tid, cas)
            znani = ig.setdefault(tid, {})
            for pid in pidi:
                znani.setdefault(pid, cas)
                (od.get(tid) or {}).pop(pid, None)       # vrnil se je
            for pid in znani:
                if pid not in pidi:
                    od.setdefault(tid, {}).setdefault(pid, cas)


def zacetki_lig():
    """Konec 1. kroga vsake lige (ISO, UTC) — od tu naprej je prijava "med sezono".

    Prvi krog se pogosto raztegne cez nekaj dni; prijave v tem casu so se
    predsezonske. Ce je ena tekma 1. kroga prestavljena za vec tednov, mejo
    omejimo na teden po prvi tekmi, da se ne premakne s prestavljeno tekmo.
    """
    out = {}
    for lg in LIGE:
        try:
            d = json.load(open(os.path.join(DATA, f'{lg}_stats.json'), encoding='utf-8'))
        except Exception:
            continue
        casi = [iso(m['dateTime']) for m in d.get('allMatches') or [] if m.get('dateTime')]
        krog1 = [iso(m['dateTime']) for m in d.get('allMatches') or []
                 if m.get('dateTime') and m.get('round') == 1]
        if not casi:
            continue
        prva = datetime.fromisoformat(min(casi).replace('Z', '+00:00'))
        teden = (prva + timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ')
        out[lg] = min(max(krog1 or casi[:1]), teden)
    return out


def iso(cas):
    """Enotna oblika (UTC, Z), da primerjava nizov ustreza primerjavi casov."""
    return (datetime.fromisoformat(cas.replace('Z', '+00:00'))
            .astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))


def je_nova(st, zac, lg, tid, pid):
    cas = st['igralci'].get(lg, {}).get(tid, {}).get(pid)
    return (lg in zac and cas is not None and cas > zac[lg]
            and cas != st['ekipe'][lg].get(tid))


def izpelji(st, rost, zac):
    """Seznam novih prijav po ligah — to bere stran."""
    # Podatki o igralcih: trenutni sestav, za odjavljene pa zadnji znani zapis.
    podatki, kje = {}, {}
    for lg, L in (rost.get('leagues') or {}).items():
        for t in L.get('teams', []):
            for p in t.get('players', []):
                podatki[(lg, str(t['id']), str(p['pid']))] = (p, t['name'])
                kje.setdefault(str(p['pid']), []).append({'lg': lg, 'team': t['name'], 'tid': t['id']})
    nove = {}
    for lg, ekipe in st['igralci'].items():
        if lg not in zac:
            continue
        vrsta = []
        for tid, znani in ekipe.items():
            for pid, cas in znani.items():
                if not je_nova(st, zac, lg, tid, pid):
                    continue
                p, ekipa = podatki.get((lg, tid, pid), (None, None))
                if not p:
                    p, ekipa = st.get('arhiv', {}).get(f'{lg}/{tid}/{pid}', (None, None))
                if not p:
                    continue
                vrsta.append({
                    'pid': int(pid), 'name': p.get('name'), 'born': p.get('born'),
                    'nat': p.get('nat'), 'natCode': p.get('natCode'), 'photo': p.get('photo'),
                    'teamId': int(tid), 'team': ekipa, 'datum': cas,
                    'odjava': (st['odjave'].get(lg, {}).get(tid) or {}).get(pid),
                    # Dvojna registracija (npr. se pri matičnem klubu) — brez tega
                    # je videti, kot da je igralec klub zamenjal.
                    'tudi': [{'lg': x['lg'], 'team': x['team']} for x in kje.get(pid, [])
                             if x['tid'] != int(tid)],
                })
        vrsta.sort(key=lambda x: (x['datum'], x['name'] or ''), reverse=True)
        nove[lg] = vrsta
    return nove


def zapomni_arhiv(st, rost, zac):
    """Zapis novega igralca, dokler je v sestavu — da imamo ime in sliko tudi po odjavi."""
    arh = st.setdefault('arhiv', {})
    for lg, L in (rost.get('leagues') or {}).items():
        for t in L.get('teams', []):
            for p in t.get('players', []):
                if je_nova(st, zac, lg, str(t['id']), str(p['pid'])):
                    arh[f"{lg}/{t['id']}/{p['pid']}"] = [
                        {x: p.get(x) for x in ('name', 'born', 'nat', 'natCode', 'photo')}, t['name']]


def zgodovina(sid):
    """Vsi posnetki rosters.json iz git zgodovine, od najstarejsega."""
    hashi = subprocess.run(['git', 'log', '--reverse', '--format=%H %cI', '--', f'{DATA}/rosters.json'],
                           capture_output=True, text=True, check=True).stdout.split('\n')
    for vrstica in filter(None, hashi):
        h, cas_commita = vrstica.split()
        try:
            rost = json.loads(subprocess.run(['git', 'show', f'{h}:{DATA}/rosters.json'],
                                             capture_output=True, text=True, check=True).stdout)
        except Exception:
            continue
        if rost.get('seasonId') != sid:
            continue
        yield rost, iso(rost.get('generated') or cas_commita)


def main():
    rost = json.load(open(os.path.join(DATA, 'rosters.json'), encoding='utf-8'))
    sid = rost.get('seasonId')
    zac = zacetki_lig()
    if '--zgodovina' in sys.argv:
        st = prazno(sid)
        n = 0
        for r, cas in zgodovina(sid):
            vnesi(st, r, cas)
            zapomni_arhiv(st, r, zac)
            n += 1
        print(f'  zgodovina: {n} posnetkov sestavov')
    else:
        st = nalozi_stanje(sid)
    vnesi(st, rost, iso(rost['generated']))
    zapomni_arhiv(st, rost, zac)
    nove = izpelji(st, rost, zac)
    st['zacetek'] = zac
    st['nove'] = nove
    st['generated'] = iso(rost['generated'])
    for lg, v in nove.items():
        print(f'  {lg}: {len(v)} novih prijav po 1. krogu ({zac[lg][:10]})')
        for x in v[:5]:
            print(f"     {x['datum'][:10]}  {x['name']:28s} {x['team']}"
                  + (f"  (odjavljen {x['odjava'][:10]})" if x['odjava'] else ''))
    with open(POT, 'w', encoding='utf-8') as f:
        json.dump(st, f, ensure_ascii=False, separators=(',', ':'))
    print(f'  ✅ {POT} ({os.path.getsize(POT) // 1024} KB)')


if __name__ == '__main__':
    main()
