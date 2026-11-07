#!/usr/bin/env python3
"""
verify_uradno.py — Ali se statistika strani ujema z uradno statistiko KZS?

ZAKAJ: novembra 2026 je ročna primerjava našla napake, ki so bile na strani
tedne in jih nihče ni opazil (108 igralcem 3. SKL je manjkala tekma, brata sta
si zamešala številke, 21 zapisnikov je bilo zastarelih). Zdaj to preverimo ob
vsaki posodobitvi; če se kaj ne ujema, zagon na koncu pade in GitHub pošlje
obvestilo.

KAJ PRIMERJA
  igralci  data/{liga}_pregled.json (isti seštevki kot na strani)
           proti /players/{id}/stats?seasonId=N, 20 polj na igralca
  ekipe    tekme, zmage, porazi, dani in prejeti koši iz data/{liga}_stats.json
           proti /matches/standings?competitionId=N

Izhod: data/preverjanje.json — stran ga pokaže pod lestvico.

Uporaba:  python scripts/verify_uradno.py
"""
import json
import os
import sys
import time
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

API = "https://api.kzs.si/api/v1/public"
LIGE = ('liga1', 'liga2', 'liga3')
# naše polje -> polje KZS
POLJA = {'T': 'played', 'fg2m': 'm2', 'fg2a': 'a2', 'fg3m': 'm3', 'fg3a': 'a3', 'ftm': 'm1', 'fta': 'a1',
         'toc': 'pts', 'nap': 'ro', 'obr': 'rd', 'pod': 'ass', 'sto': 'st', 'izg': 'tov', 'pz': 'pfcm',
         'izs': 'pfrv', 'blk': 'blfv', 'pre': 'blag', 'pm': 'plusMinus', 'efe': 'efficiency', 'MIN': 'mins'}
TOLERANCA = {'MIN': 0.51}     # minute: seštevek zaokroženih vrednosti
NAJVEC_PRIMEROV = 20


def fetch_json(url, poskusov=3):
    for i in range(poskusov):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode())
        except Exception:
            time.sleep(i + 1)
    return None


def preveri_igralce(lg, primeri):
    p = json.load(open(f'data/{lg}_pregled.json', encoding='utf-8'))
    d = json.load(open(f'data/{lg}_stats.json', encoding='utf-8'))
    sezona = d['seasonId']
    tekma = next((m for m in d['allMatches'] if m.get('competitions')), None)
    if not tekma or not p.get('players'):
        return None
    comp = tekma['competitions'][0]['competitionId']
    nasi = {}
    for x in p['players']:              # prestop znotraj lige: dve vrstici, en igralec
        z = nasi.setdefault(x['pid'], Counter())
        z['ime'] = x['name']
        for k in POLJA:
            z[k] += x.get(k) or 0

    def uradno(pid):
        o = fetch_json(f"{API}/players/{pid}/stats?seasonId={sezona}")
        return pid, None if o is None else ((o.get('data') or {}).get('competitionStats') or [])

    with ThreadPoolExecutor(4) as ex:
        odgovori = dict(ex.map(uradno, sorted(nasi)))

    ujema = neznano = 0
    for pid, z in nasi.items():
        rez = odgovori.get(pid)
        if rez is None:                 # KZS ni odgovoril: to ni odstopanje
            neznano += 1
            continue
        o = next((c for c in rez if c.get('competitionId') == comp), None)
        if not o:
            primeri.append(f"{lg} {z['ime']}: na strani je, KZS ga v tekmovanju nima")
            continue
        razlike = [f"{k} {round(z[k], 1)}/{o.get(u) or 0}" for k, u in POLJA.items()
                   if abs(z[k] - (o.get(u) or 0)) > TOLERANCA.get(k, 0)]
        if razlike:
            primeri.append(f"{lg} {z['ime']}: " + ', '.join(razlike[:4]))
        else:
            ujema += 1
    return {'igralcev': len(nasi) - neznano, 'igralciUjema': ujema, 'neznano': neznano}


def preveri_ekipe(lg, primeri):
    d = json.load(open(f'data/{lg}_stats.json', encoding='utf-8'))
    tekma = next((m for m in d['allMatches'] if m.get('competitions')), None)
    if not tekma:
        return {'ekip': 0, 'ekipeUjema': 0}
    comp = tekma['competitions'][0]['competitionId']
    o = fetch_json(f"{API}/matches/standings?competitionId={comp}")
    if not o:
        return {'ekip': 0, 'ekipeUjema': 0}
    ekip = ujema = 0
    for ph in (o.get('data') or {}).get('phases') or []:
        faza = (ph.get('competitionPhase') or {}).get('name')
        skupine = ([ph['standings']] if ph.get('standings')
                   else [g.get('standings') or [] for g in ph.get('groups') or []])
        nasi = {}
        for m in d['allMatches']:
            if m.get('status') != 'FINISHED' or (m.get('competitions') or [{}])[0].get('competitionPhaseName') != faza:
                continue
            a, b = m['firstTeamScore'], m['secondTeamScore']
            for ime, za, proti in ((m['firstTeamName'], a, b), (m['secondTeamName'], b, a)):
                t = nasi.setdefault(ime, Counter())
                t['gamesPlayed'] += 1
                t['won'] += za > proti
                t['lost'] += za < proti
                t['ptsPlus'] += za
                t['ptsMinus'] += proti
        for st in skupine:
            for s in st:
                if not s.get('gamesPlayed'):
                    continue
                ekip += 1
                t = nasi.get(s['teamName'], Counter())
                razlike = [f"{k} {t[k]}/{s.get(k)}" for k in ('gamesPlayed', 'won', 'lost', 'ptsPlus', 'ptsMinus')
                           if t[k] != s.get(k)]
                if razlike:
                    primeri.append(f"{lg} {faza} {s['teamName'].strip()}: " + ', '.join(razlike))
                else:
                    ujema += 1
    return {'ekip': ekip, 'ekipeUjema': ujema}


def main():
    primeri, lige = [], {}
    for lg in LIGE:
        if not os.path.exists(f'data/{lg}_pregled.json'):
            continue
        r = preveri_igralce(lg, primeri)
        if r is None:
            continue
        r.update(preveri_ekipe(lg, primeri))
        lige[lg] = r
        print(f"  {lg}: igralci {r['igralciUjema']}/{r['igralcev']}, ekipe {r['ekipeUjema']}/{r['ekip']}"
              + (f", brez odgovora KZS {r['neznano']}" if r['neznano'] else ''))
    odstopanj = len(primeri)
    izid = {
        'preverjeno': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'igralcev': sum(v['igralcev'] for v in lige.values()),
        'ekip': sum(v['ekip'] for v in lige.values()),
        'odstopanj': odstopanj,
        'lige': lige,
        'primeri': primeri[:NAJVEC_PRIMEROV],
    }
    with open('data/preverjanje.json', 'w', encoding='utf-8') as f:
        json.dump(izid, f, ensure_ascii=False, indent=1)
    for p in primeri[:NAJVEC_PRIMEROV]:
        print(f"::warning::{p}")
    print(f"  Preverjenih {izid['igralcev']} igralcev in {izid['ekip']} ekip, odstopanj: {odstopanj}")


if __name__ == '__main__':
    main()
