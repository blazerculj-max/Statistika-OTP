#!/usr/bin/env python3
"""
ovr_prior.py — Lanska kakovost igralcev kot izhodisce za OVR na 2K kartici.

ZAKAJ: po nekaj kolih je letosnji vzorec majhen. OVR je bil doslej samo
preslikava bElo; ta po treh kolih komaj zapusti zacetnih 1500, zato je bil
razpon vseh igralcev 71-81 in je npr. rezervist s 4 tockami na tekmo dobil
vec kot nosilec ekipe s 16. Lanska sezona je dober dokaz, kdo je kdo — z
vsako letosnjo tekmo pa steje manj (mesanje je v index.html, izracunajOVR).

KAKOVOST q (0..1) v ligi in sezoni, ista formula kot v index.html:
  q = 0,60 x percentil(Game Score na tekmo)        — koliko prinese
    + 0,25 x percentil(Game Score na 36 minut)      — kako ucinkovito
    + 0,15 x percentil(minute na tekmo)             — kaksno vlogo ima
  3. SKL nima minut ne poskusov: q = percentil(Game Score na tekmo).
  Game Score (Hollinger) = TOC + 0,4 FG - 0,7 FGA - 0,4 (FTA-FT) + 0,7 OR
                         + 0,3 DR + STL + 0,7 AST + 0,7 BLK - 0,4 PF - TOV

Izhod: data/ovr_prior.json  {"sezona": 26, "lige": {"liga1": {pid: {"q":.., "t":..}}}}
Kljuc je playerId KZS — med sezonami je stalen (preverjeno: 356 igralcev).

Uporaba:  python scripts/ovr_prior.py            (prejsnja sezona iz seasons.json)
          python scripts/ovr_prior.py --sezona 26
"""
import json
import os
import sys
from collections import defaultdict

LIGE = ('liga1', 'liga2', 'liga3')
MIN_MINUT_ZA_UCINKOVITOST = 40   # pod tem je GS/36 sum (en met v dveh minutah)


def arg(ime, privzeto=None):
    return sys.argv[sys.argv.index(ime) + 1] if ime in sys.argv[:-1] else privzeto


def game_score(p):
    fgm = (p.get('twoPM') or 0) + (p.get('threePM') or 0)
    fga = (p.get('twoPA') or 0) + (p.get('threePA') or 0)
    return ((p.get('points') or 0) + 0.4 * fgm - 0.7 * fga
            - 0.4 * ((p.get('fTA') or 0) - (p.get('fTM') or 0))
            + 0.7 * (p.get('offensiveRebounds') or 0) + 0.3 * (p.get('defensiveRebounds') or 0)
            + (p.get('steals') or 0) + 0.7 * (p.get('assists') or 0)
            + 0.7 * (p.get('blocksInFavor') or 0) - 0.4 * (p.get('foulCommited') or 0)
            - (p.get('turnovers') or 0))


def percentil(vrednosti):
    """vrednost -> delez igralcev z manjso vrednostjo (0..1), enake vrednosti dobijo isti rang."""
    s = sorted(vrednosti)
    n = len(s)
    out = {}
    for v in set(s):
        manj = sum(1 for x in s if x < v)
        enako = sum(1 for x in s if x == v)
        out[v] = (manj + (enako - 1) / 2) / max(n - 1, 1)
    return out


def kakovost(pot):
    d = json.load(open(pot, encoding='utf-8'))
    igr = defaultdict(lambda: {'t': 0, 'gs': 0.0, 'min': 0.0})
    for md in (d.get('matchStats') or {}).values():
        for stran in ('firstTeam', 'secondTeam'):
            for p in ((md.get(stran) or {}).get('playerStats') or []):
                if not p.get('played'):
                    continue
                r = igr[p['playerId']]
                r['t'] += 1
                r['gs'] += game_score(p)
                r['min'] += p.get('minutes') or 0
    if not igr:
        return {}
    ima_minute = sum(r['min'] for r in igr.values()) > 0
    gs_pg = {pid: r['gs'] / r['t'] for pid, r in igr.items()}
    p_gs = percentil(gs_pg.values())
    out = {}
    if ima_minute:
        min_pg = {pid: r['min'] / r['t'] for pid, r in igr.items()}
        gs36 = {pid: r['gs'] / r['min'] * 36 for pid, r in igr.items() if r['min'] >= MIN_MINUT_ZA_UCINKOVITOST}
        p_min = percentil(min_pg.values())
        p_36 = percentil(gs36.values())
        for pid, r in igr.items():
            ucink = p_36[gs36[pid]] if pid in gs36 else p_gs[gs_pg[pid]]
            q = 0.60 * p_gs[gs_pg[pid]] + 0.25 * ucink + 0.15 * p_min[min_pg[pid]]
            out[str(pid)] = {'q': round(q, 4), 't': r['t']}
    else:
        for pid, r in igr.items():
            out[str(pid)] = {'q': round(p_gs[gs_pg[pid]], 4), 't': r['t']}
    return out


def main():
    sez = arg('--sezona')
    if not sez:
        trenutna = json.load(open('data/seasons.json', encoding='utf-8')).get('current')
        sez = str(int(trenutna) - 1)
    rez = {'sezona': int(sez), 'lige': {}}
    for lg in LIGE:
        pot = f'data/{lg}_stats_s{sez}.json'
        if not os.path.exists(pot):
            print(f'  - {pot} ne obstaja')
            continue
        rez['lige'][lg] = kakovost(pot)
        print(f'  {lg}: {len(rez["lige"][lg])} igralcev')
    with open('data/ovr_prior.json', 'w', encoding='utf-8') as f:
        json.dump(rez, f, separators=(',', ':'))
    print(f'  ✅ data/ovr_prior.json ({os.path.getsize("data/ovr_prior.json") // 1024} KB)')


if __name__ == '__main__':
    main()
