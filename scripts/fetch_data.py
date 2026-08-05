#!/usr/bin/env python3
"""
KZS Data Fetcher — Incremental, brez PBP (PBP je ločen job).

Generira data/{key}_pregled.json — majhen JSON (~200-400 KB) z že-agregiranimi
igralci (totali) + tekmami + game logom. Aplikacija ga naloži PRVEGA → Pregled
se izriše takoj, veliki _stats.json pa se naloži v ozadju. bElo/power/MVP še
vedno računa brskalnik (iz teh totalov), zato ni podvajanja logike.

SEZONA IN ID-ji SE ODKRIJEJO SAMI:
  - Sezona: najnovejša z objavljenim koledarjem (ali --season N / env SEASON_ID).
  - competitionId in vse faze (vključno s končnico, ki jo KZS doda med sezono)
    se preberejo iz /competitions/?seasonId=N in /competitions/{id}.
  Trdo kodiranih phase_ids in seznamov ekip ni več — ob novi sezoni ni treba
  ničesar ročno posodabljati.
"""

import json, time, urllib.request, os, sys
from datetime import datetime, timezone, timedelta

API_BASE  = "https://api.kzs.si/api/v1/public"
FIBA_BASE = "https://fibalivestats.dcd.shared.geniussports.com/data"

# Katero tekmovanje je katera liga — po imenu + spolu + rangu (stabilno čez sezone).
LEAGUES = {
    'liga1': {'name': 'Liga OTP banka', 'gender': 'MALE', 'rank': 'FIRST'},
    'liga2': {'name': '2. SKL',         'gender': 'MALE', 'rank': 'SECOND'},
    'liga3': {'name': '3. SKL',         'gender': 'MALE', 'rank': 'THIRD'},
}

SEASON_LABELS = {22:'2021/22',23:'2022/23',24:'2023/24',25:'2024/25',26:'2025/26',
                 27:'2026/27',28:'2027/28',29:'2028/29',30:'2029/30'}

# Koliko najnovejših sezon preverimo, preden obupamo (KZS ima vnaprej
# ustvarjene prazne sezone, zato jih je treba nekaj preskočiti).
MAX_SEASON_LOOKBACK = 6

FORCE_FULL  = '--full'  in sys.argv
FETCH_PBP   = '--pbp'   in sys.argv
STATS_ONLY  = '--stats' in sys.argv

def fetch_json(url, retries=3):
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            print(f"  Err ({i+1}): {e}")
            time.sleep(i + 1)
    return None

# ════════════════════════════════════════════════════════════
# ODKRIVANJE SEZONE IN TEKMOVANJ
# ════════════════════════════════════════════════════════════
def _arg_season():
    if '--season' in sys.argv:
        i = sys.argv.index('--season')
        if i + 1 < len(sys.argv):
            return int(sys.argv[i + 1])
    if os.environ.get('SEASON_ID'):
        return int(os.environ['SEASON_ID'])
    return None

def find_competitions(season_id):
    """{key: competitionId} — poišče naše lige po imenu + spolu + rangu (1 klic)."""
    d = fetch_json(f"{API_BASE}/competitions/?seasonId={season_id}")
    items = (d or {}).get('data', {}).get('items', [])
    found = {}
    for key, spec in LEAGUES.items():
        for c in items:
            if (c.get('name') == spec['name'] and c.get('gender') == spec['gender']
                    and c.get('rank') == spec['rank']):
                found[key] = c['id']
                break
    return found

def fetch_phases(comp_id):
    """[{'id':…, 'name':…, 'groups':[gid,…]}] — vključno s končnico, če že obstaja."""
    d = fetch_json(f"{API_BASE}/competitions/{comp_id}")
    out = []
    for p in (d or {}).get('data', {}).get('phases', []):
        out.append({'id': p['id'], 'name': p.get('name', ''),
                    'groups': [g['id'] for g in p.get('groups', []) if g.get('id')]})
    return out

def resolve_season():
    """
    Vrne (season_id, {key: {'id':…, 'name':…, 'phases':[…]}}).
    Brez --season/SEASON_ID vzame najnovejšo sezono, ki ima že objavljen koledar
    1. lige — tako se ob začetku nove sezone preklopi sama.
    """
    forced = _arg_season()
    d = fetch_json(f"{API_BASE}/seasons/")
    items = (d or {}).get('data', {}).get('items', [])
    # POZOR: ID-ji sezon pri KZS NISO kronološki (npr. id 39 = 1999/2000),
    # zato razvrščamo po začetni letnici iz imena ("2026/2027" → 2026).
    # KZS ima tudi vnaprej ustvarjene prazne sezone (2027/28 …), zato gremo po
    # vrsti navzdol do prve, ki ima tekmovanja IN objavljen koledar.
    def start_year(s):
        try:
            return int(str(s.get('name', ''))[:4])
        except ValueError:
            return 0
    newest = sorted(items, key=start_year, reverse=True)
    candidates = [forced] if forced else [s['id'] for s in newest[:MAX_SEASON_LOOKBACK]]

    for sid in candidates:
        ids = find_competitions(sid)
        if 'liga1' not in ids:
            if not forced:
                continue          # prazna/vnaprej ustvarjena sezona
            raise SystemExit(f"Sezona {sid}: ne najdem tekmovanja '{LEAGUES['liga1']['name']}'")
        if not forced:
            # sprejmi le sezono, ki ima že objavljen koledar 1. lige
            probe = fetch_json(f"{API_BASE}/matches/?competitionId={ids['liga1']}&seasonId={sid}&limit=1")
            if not (probe or {}).get('data', {}).get('items'):
                print(f"  (sezona {sid}: koledar še ni objavljen — preverjam starejšo)")
                continue
        comps = {key: {'id': cid, 'name': LEAGUES[key]['name'], 'phases': fetch_phases(cid)}
                 for key, cid in ids.items()}
        return sid, comps

    raise SystemExit("Ne najdem sezone z objavljenim koledarjem. Podaj ročno: --season N")

# POZOR: KZS API IGNORIRA parameter &page — vsaka "stran" vrne isti prvi sveženj.
# Delujoč način strani je &offset (in &limit sprejme tudi vrednosti nad 200).
# Prav tako je &competitionPhaseGroupId pri nekaterih tekmovanjih tiho ignoriran,
# zato pobiramo po FAZAH, skupino pa preberemo iz podatkov same tekme.
PAGE_LIMIT = 1000

def fetch_phase(comp_id, phase_id=None, limit=PAGE_LIMIT, max_pages=50):
    all_items = []
    offset = 0
    for _ in range(max_pages):
        url = (f"{API_BASE}/matches/?competitionId={comp_id}&seasonId={SEASON_ID}"
               f"&limit={limit}&offset={offset}")
        if phase_id: url += f"&competitionPhaseId={phase_id}"
        data = fetch_json(url)
        items = data.get('data', {}).get('items', []) if data else []
        if not items: break
        all_items.extend(items)
        if len(items) < limit: break
        offset += limit
        time.sleep(0.1)
    return all_items

def fetch_all_matches(key, lg):
    """
    Pobere vse tekme lige: za vsako odkrito fazo posebej, nato odstrani dvojnike
    in tekme drugih tekmovanj. Filter po competitionId nadomešča nekdanje
    sezonske sezname ekip.
    """
    all_items = []
    for ph in lg['phases']:
        all_items.extend(fetch_phase(lg['id'], ph['id']))
    if not lg['phases']:   # faz (še) ni — poberi celo tekmovanje naenkrat
        all_items = fetch_phase(lg['id'])

    seen = set()
    matches = [m for m in all_items if not (m['id'] in seen or seen.add(m['id']))]
    matches = [m for m in matches
               if any(c.get('competitionId') == lg['id']
                      for c in m.get('competitions', [{}]))]

    # id kot zadnje merilo → izpis je enak ne glede na vrstni red pobiranja faz
    matches.sort(key=lambda m: (m['round'], m.get('dateTime', ''), m['id']))
    return matches

def load_existing_stats(key):
    """Obstoječi podatki — a le, če so iz ISTE sezone. Ob prehodu v novo sezono
    začnemo prazno, da se stara sezona ne vleče naprej (arhiv jo že hrani)."""
    path = f"data/{key}_stats.json"
    if not os.path.exists(path):
        return None
    with open(path) as f:
        data = json.load(f)
    prev = data.get('seasonId')
    if prev is not None and prev != SEASON_ID:
        print(f"  ↻ nova sezona ({prev} → {SEASON_ID}) — {path} začenjam prazno")
        return None
    return data

def load_existing_pbp(key):
    path = f"data/{key}_pbp.json"
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        data = json.load(f)
    prev = data.get('seasonId')
    if prev is not None and prev != SEASON_ID:
        return {}
    return data.get('pbp', {})

def needs_full_fetch(existing):
    if FORCE_FULL or not existing:
        return True
    updated = datetime.fromisoformat(existing['updatedAt'].replace('Z', '+00:00'))
    age_h = (datetime.now(timezone.utc) - updated).total_seconds() / 3600
    return age_h > 24 * 7

def fetch_stats_incremental(matches, existing_stats):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
    needs = []
    for m in matches:
        if m['status'] != 'FINISHED': continue
        mid = str(m['id'])
        has = mid in existing_stats
        try:
            dt = datetime.fromisoformat(m['dateTime'].replace('Z', '+00:00'))
        except:
            dt = datetime.min.replace(tzinfo=timezone.utc)
        if not has or dt > cutoff:
            needs.append(m)

    if not needs:
        print(f"  Stats: vse shranjeno ({len(existing_stats)})")
        return existing_stats

    print(f"  Stats: fetcham {len(needs)} tekem...")
    stats = dict(existing_stats)
    for i, m in enumerate(needs):
        d = fetch_json(f"{API_BASE}/matches/{m['id']}/stats")
        if d and d.get('data'):
            stats[str(m['id'])] = d['data']
        if (i+1) % 10 == 0:
            print(f"  Stats {i+1}/{len(needs)}")
        time.sleep(0.1)
    return stats

def fetch_pbp_incremental(matches, existing_pbp):
    needs = [m for m in matches
             if m['status'] == 'FINISHED'
             and m.get('fibaLiveStatsUrl')
             and str(m['id']) not in existing_pbp]

    if not needs:
        print(f"  PBP: vse shranjeno ({len(existing_pbp)})")
        return existing_pbp

    print(f"  PBP: fetcham {len(needs)} novih tekem...")
    pbp = dict(existing_pbp)
    for i, m in enumerate(needs):
        fid = m['fibaLiveStatsUrl'].rstrip('/').split('/')[-1]
        d = fetch_json(f"{FIBA_BASE}/{fid}/data.json")
        if d and d.get('pbp'):
            pbp[str(m['id'])] = [
                {k: ev.get(k,'') for k in
                 ('gt','period','periodType','lead','tno','actionType','subType','success','firstName','familyName')}
                for ev in d['pbp']
                if ev.get('actionType') in ('2pt','3pt','freethrow','turnover','assist','rebound','block','steal')
            ]
        if (i+1) % 5 == 0:
            print(f"  PBP {i+1}/{len(needs)}")
        time.sleep(0.4)
    return pbp

# ════════════════════════════════════════════════════════════
# PREGLED JSON — agregirani igralci (totali) za hitro nalaganje
# Zrcali aggregatePlayers() v HTML. Brez surovih matchStats.
# Aplikacija iz tega izračuna bElo/power/MVP (ista logika kot prej).
# ════════════════════════════════════════════════════════════
def normalize_team(name):
    # zrcali normalizeTeamName v HTML (osnovna verzija — trim).
    # Če imaš v HTML posebne preslikave imen, jih dodaj sem.
    return (name or '').strip()

def build_pregled(matches, stats):
    """Vrne kompakten dict: tekme (minimalno) + agregirani igralci (totali) + game log."""
    valid_ids = set(m['id'] for m in matches)
    round_by_id = {m['id']: m.get('round', 0) for m in matches}

    byKey = {}      # pid_team → totali
    gamelog = {}    # pid → [{round,matchId,team,pts,reb,ast,efe,min}]

    for md in stats.values():
        if not md: continue
        mid = md.get('matchId')
        if mid not in valid_ids: continue
        rnd = round_by_id.get(mid, 0)
        for teamData in (md.get('firstTeam'), md.get('secondTeam')):
            if not teamData or not teamData.get('playerStats'): continue
            team = normalize_team(teamData.get('teamName'))
            for ps in teamData['playerStats']:
                has_stats = (ps.get('points',0)>0 or ps.get('twoPM',0)>0 or
                             ps.get('threePM',0)>0 or ps.get('fTM',0)>0)
                if not ps.get('played') or (not has_stats and ps.get('minutes',0)==0):
                    continue
                pid = ps.get('playerId')
                key = f"{pid}_{team}"
                if key not in byKey:
                    byKey[key] = {
                        'name': (ps.get('matchTeamPlayerFirstname','').strip()+' '+
                                 ps.get('matchTeamPlayerLastname','').strip()).strip(),
                        'team': team,
                        'born': int(ps.get('matchTeamPlayerYearOfBirth') or 0),
                        'pid': pid,
                        'T':0,'MIN':0,'fg2m':0,'fg2a':0,'fg3m':0,'fg3a':0,'ftm':0,'fta':0,
                        'toc':0,'nap':0,'obr':0,'pod':0,'sto':0,'izs':0,'izg':0,'pz':0,
                        'blk':0,'pre':0,'pm':0,'efe':0,'pip':0,'fb':0,'sc':0
                    }
                p = byKey[key]
                p['T']+=1; p['MIN']+=ps.get('minutes',0)
                p['fg2m']+=ps.get('twoPM',0); p['fg2a']+=ps.get('twoPA',0)
                p['fg3m']+=ps.get('threePM',0); p['fg3a']+=ps.get('threePA',0)
                p['ftm']+=ps.get('fTM',0); p['fta']+=ps.get('fTA',0)
                p['toc']+=ps.get('points',0)
                p['nap']+=ps.get('offensiveRebounds',0); p['obr']+=ps.get('defensiveRebounds',0)
                p['pod']+=ps.get('assists',0); p['sto']+=ps.get('steals',0)
                p['izg']+=ps.get('turnovers',0); p['pz']+=ps.get('foulCommited',0)
                p['blk']+=ps.get('blocksInFavor',0); p['pre']+=ps.get('blocksAgainst',0)
                p['pm']+=ps.get('plusMinus',0)
                p['efe']+=(ps.get('efficiencyCustom') or ps.get('efficiency') or 0)
                p['pip']+=ps.get('pointsInThePaint',0)
                p['fb']+=ps.get('pointsFastBreak',0)
                p['sc']+=ps.get('pointsSecondChance',0)
                p['izs']+=ps.get('foulReceived',0)
                # game log (za trend / MVP po krogih)
                gl = gamelog.setdefault(pid, [])
                if not any(g['matchId']==mid for g in gl):
                    gl.append({
                        'round':rnd,'matchId':mid,'team':team,
                        'pts':ps.get('points',0),
                        'reb':ps.get('offensiveRebounds',0)+ps.get('defensiveRebounds',0),
                        'ast':ps.get('assists',0),
                        'efe':ps.get('efficiencyCustom') or ps.get('efficiency') or 0,
                        'min':ps.get('minutes',0)
                    })

    # tekme — samo polja, ki jih Pregled rabi (rezultati, imena, datum, faza)
    slim_matches = [{
        'id':m['id'],'round':m.get('round',0),'status':m['status'],
        'dateTime':m.get('dateTime',''),
        'firstTeamName':m['firstTeamName'],'secondTeamName':m['secondTeamName'],
        'firstTeamScore':m.get('firstTeamScore'),'secondTeamScore':m.get('secondTeamScore'),
        'firstTeamLogoUuid':m.get('firstTeamLogoUuid'),'secondTeamLogoUuid':m.get('secondTeamLogoUuid'),
    } for m in matches]

    players = list(byKey.values())

    # MVP — že-izračunan (zrcali calcMvpScore + getLeaguePlayers v HTML),
    # da je junak na Pregledu ZAGOTOVO isti kot pri polnih podatkih.
    n_finished = sum(1 for m in matches if m['status']=='FINISHED')
    min_t = max(1, round(n_finished*0.3/5))
    grouped = {}
    for p in players:
        k = f"{p['name']}_{p['born']}"
        if k not in grouped:
            grouped[k] = dict(p)
        else:
            for fld in ('T','MIN','fg2m','fg2a','fg3m','fg3a','ftm','fta','toc','nap','obr',
                        'pod','sto','izs','izg','pz','blk','pre','pm','efe','pip','fb','sc'):
                grouped[k][fld] += p[fld]
    def mvp_score(p):
        t = max(p['T'],1)
        return (p['toc']/t)*1.0+((p['nap']+p['obr'])/t)*0.7+(p['pod']/t)*0.8+ \
               (p['sto']/t)*1.5+(p['blk']/t)*1.5-(p['izg']/t)*0.8+(p['efe']/t)*0.3
    elig = [p for p in grouped.values() if p['T']>=min_t]
    mvp_pid = max(elig, key=mvp_score)['pid'] if elig else None

    return {'players':players, 'matches':slim_matches, 'gamelog':gamelog, 'mvpPid':mvp_pid}

def process_league(key, lg):
    phase_desc = ', '.join(f"{p['name']}({p['id']})" for p in lg['phases']) or 'brez faz'
    print(f"\n--- {lg['name']} · comp {lg['id']} · {phase_desc} ---")
    existing = load_existing_stats(key)

    matches = fetch_all_matches(key, lg)
    finished = [m for m in matches if m['status'] == 'FINISHED']
    live     = [m for m in matches if m['status'] == 'LIVE']
    print(f"  {len(matches)} tekem | {len(finished)} končanih | {len(live)} v živo")
    print(f"  Ekipe: {len(set(m['firstTeamName'] for m in matches))}")

    # Varovalka: če API vrne nič tekem, obstoječih datotek NE povozimo s praznimi.
    if not matches and os.path.exists(f"data/{key}_stats.json"):
        print(f"  ⚠ API ni vrnil nobene tekme — obdržim obstoječe datoteke")
        return []

    existing_stats = existing.get('matchStats', {}) if existing else {}
    stats = fetch_stats_incremental(matches, existing_stats)

    now = datetime.now(timezone.utc).isoformat()
    payload = {'updatedAt': now, 'league': lg['name'], 'seasonId': SEASON_ID,
               'allMatches': matches, 'matchStats': stats}
    stats_file = f"data/{key}_stats.json"
    with open(stats_file, 'w') as f:
        json.dump(payload, f, ensure_ascii=False, separators=(',',':'))
    print(f"  ✅ {stats_file} ({os.path.getsize(stats_file)//1024} KB)")

    # ── PREGLED JSON (majhen, za hitro prvo nalaganje) ──
    try:
        pregled = build_pregled(matches, stats)
        pregled['updatedAt'] = now
        pregled['league'] = lg['name']
        pregled_file = f"data/{key}_pregled.json"
        with open(pregled_file, 'w') as f:
            json.dump(pregled, f, ensure_ascii=False, separators=(',',':'))
        print(f"  ⚡ {pregled_file} ({os.path.getsize(pregled_file)//1024} KB, {len(pregled['players'])} igralcev)")
    except Exception as e:
        print(f"  ! pregled.json: {e}")

    # Verzionirana kopija sezone (za arhiv + medsezonsko primerjavo)
    season_file = f"data/{key}_stats_s{SEASON_ID}.json"
    with open(season_file, 'w') as f:
        json.dump(payload, f, ensure_ascii=False, separators=(',',':'))
    print(f"  📅 {season_file} (arhiv sezone {SEASON_ID})")

    # Posodobi seasons.json manifest
    try:
        sm_path = "data/seasons.json"
        sm = {}
        if os.path.exists(sm_path):
            with open(sm_path) as f: sm = json.load(f)
        seasons = sm.get('seasons', {})
        entry = seasons.get(str(SEASON_ID), {'id': SEASON_ID, 'name': SEASON_LABELS.get(SEASON_ID, f'Sezona {SEASON_ID}'), 'leagues': [], 'files': {}})
        if key not in entry['leagues']:
            entry['leagues'].append(key)
        entry['files'][key] = f"{key}_stats_s{SEASON_ID}.json"
        seasons[str(SEASON_ID)] = entry
        sm['seasons'] = seasons
        sm['current'] = SEASON_ID
        with open(sm_path, 'w', encoding='utf-8') as f:
            json.dump(sm, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  ! seasons.json: {e}")

    if FETCH_PBP:
        if key == 'liga3':
            print(f"  PBP: liga3 preskočena (FIBA blokira GitHub Actions IP)")
            pbp_file = f"data/{key}_pbp.json"
            if not os.path.exists(pbp_file):
                with open(pbp_file, 'w') as f:
                    json.dump({'updatedAt': now, 'seasonId': SEASON_ID, 'pbp': {}}, f)
        else:
            existing_pbp = load_existing_pbp(key)
            pbp = fetch_pbp_incremental(matches, existing_pbp)
            pbp_file = f"data/{key}_pbp.json"
            with open(pbp_file, 'w') as f:
                json.dump({'updatedAt': now, 'seasonId': SEASON_ID, 'pbp': pbp},
                          f, ensure_ascii=False, separators=(',',':'))
            print(f"  ✅ {pbp_file} ({os.path.getsize(pbp_file)//1024} KB)")
    else:
        print(f"  PBP: preskočen (dodaj --pbp za fetch)")

    return [
        {'matchId': m['id'], 'round': m['round'], 'date': m.get('dateTime',''),
         'home': m['firstTeamName'], 'away': m['secondTeamName'],
         'homeScore': m.get('firstTeamScore'), 'awayScore': m.get('secondTeamScore'),
         'attendance': m.get('attendance', 0), 'hall': m.get('sportHallName',''),
         'phase': m.get('competitions',[{}])[0].get('competitionPhaseName',''),
         'phaseGroup': m.get('competitions',[{}])[0].get('competitionPhaseGroupName',''),
        }
        for m in finished if m.get('attendance', 0) > 0
    ]

# ── MAIN ──
os.makedirs('data', exist_ok=True)
mode = 'FULL+PBP' if (FORCE_FULL and FETCH_PBP) else ('FULL' if FORCE_FULL else ('PBP' if FETCH_PBP else 'INCREMENTAL'))
print(f"\n=== KZS Fetcher [{mode}] {datetime.now(timezone.utc).isoformat()} ===")

SEASON_ID, COMPS = resolve_season()
_lg_desc = ', '.join('{}={}'.format(k, v['id']) for k, v in COMPS.items())
print(f"Sezona {SEASON_ID} ({SEASON_LABELS.get(SEASON_ID, '?')}) · lige: {_lg_desc}")

if '--probe' in sys.argv:      # samo pokaži, kaj bi pobral, in končaj
    for key, lg in COMPS.items():
        ms = fetch_all_matches(key, lg)
        teams = sorted({m['firstTeamName'] for m in ms} | {m['secondTeamName'] for m in ms})
        print(f"\n{key}: comp {lg['id']} | faze "
              f"{[(p['id'], p['name'], p['groups']) for p in lg['phases']]}")
        print(f"  {len(ms)} tekem, {len(teams)} ekip: {teams}")
    sys.exit(0)

all_att = {}
for key, lg in COMPS.items():
    all_att[key] = process_league(key, lg)

with open('data/attendance.json', 'w') as f:
    json.dump({'updatedAt': datetime.now(timezone.utc).isoformat(), 'leagues': all_att},
              f, ensure_ascii=False, separators=(',',':'))
print(f"\n✅ data/attendance.json")
print(f"=== Done! ===")
