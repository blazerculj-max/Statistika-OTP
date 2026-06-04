#!/usr/bin/env python3
"""
fetch_3x3.py — pobere podatke 3x3 Državc (FIBA play) za kategorijo Člani/Open
in jih inkrementalno shrani v data/drzavc_3x3.json.

Zasnova:
- Bere seznam turnirjev iz TOURNAMENTS spodaj (eventId + categoryId Open).
- Za vsak turnir pobere ekipe (category/{id}/teams) in tekme (event/{id}/games).
- Filtrira SAMO kategorijo Open (člani moški).
- Inkrementalno: zapiše samo, če se je kaj spremenilo (nova tekma/rezultat/ekipa).
- Status turnirja (upcoming/completed) določi iz tekem (so rezultati?).

Teče prek GitHub Actions (glej .github/workflows/update_3x3.yml) na dneve turnirjev.

OPOMBA: FIBA lahko blokira nekatere IP-je (bot detection). GitHub Actions IP
običajno deluje, a če ne, skript to zapiše v log in obdrži obstoječe podatke
(ne prepiše s praznim).
"""
import json, os, sys, time, urllib.request, urllib.error

FIBA = "https://play.fiba3x3.com"
OUT = os.path.join(os.path.dirname(__file__), "..", "data", "drzavc_3x3.json")
SEASON = "2026"

# ── Turnirji serije Državc (dodaj nove sem) ──
# categoryId Open se za vsak turnir RAZLIKUJE — dobi ga iz event/{id}/teams
# (poišči ekipo s categoryName "Open" → njen categoryId).
TOURNAMENTS = [
    {
        "name": "Državc Škofja Loka",
        "eventId": "e2bbe5b4-fcb3-4bc3-8341-c7a02208ee0e",
        "openCategoryId": "8512d4de-5194-4687-931d-09f82f5833f7",
        "date": "2026-05-30",
    },
    {
        "name": "Državc Vrhnika",
        "eventId": "2683f287-8996-4d6b-b58c-176a29850250",
        "openCategoryId": "7d31d477-43c8-4f14-a49b-11ab541a3dfc",
        "date": "2026-06-06",  # sobota
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "application/json",
}

NAME_FIX = {"Až Masazni studio": "AŽ masažni studio"}
def fix_name(n): return NAME_FIX.get(n, n)

def fetch_json(url, tries=3):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            print(f"  ! poskus {i+1}/{tries} ni uspel: {e}", file=sys.stderr)
            time.sleep(3)
    return None

def img_from_sso(sso):
    if not sso or sso == "00000000-0000-0000-0000-000000000000":
        return None
    a, b, c, d = sso[0], sso[1], sso[2], sso[3]
    return f"https://assets.fiba3x3.com/images/Member/{a}/{b}/{c}/{d}/{sso}/profile.80x80.png"

def parse_teams(raw):
    """category/{id}/teams → [{name, players:[{first,last,age,rank,img}]}]"""
    out = []
    for t in raw:
        players = []
        for m in t.get("teamMembers", []):
            players.append({
                "first": (m.get("firstName") or "").strip(),
                "last": (m.get("lastName") or "").strip(),
                "age": m.get("age"),
                "rank": m.get("rankingPoints", 0) or 0,
                "img": img_from_sso(m.get("ssoId")),
            })
        out.append({"name": fix_name(t.get("name", "")), "players": players})
    return out

def parse_games(raw, open_cat):
    """event/{id}/games → samo Open tekme → [{name,round,group,time,home,hs,hw,away,as,aw}]"""
    out = []
    for g in raw:
        if g.get("categoryId") != open_cat:
            continue
        ht, at = g.get("homeTeam", {}), g.get("awayTeam", {})
        dt = g.get("gameStartDatetime", "")
        tm = dt[11:16] if len(dt) >= 16 else ""  # "HH:MM"
        out.append({
            "name": g.get("gameName", ""),
            "round": g.get("roundCode", ""),
            "group": g.get("groupPoolCode", "") or "",
            "time": tm,
            "home": fix_name(ht.get("teamName", "")),
            "hs": ht.get("teamScore"),
            "hw": bool(ht.get("teamIsWinner")),
            "away": fix_name(at.get("teamName", "")),
            "as": at.get("teamScore"),
            "aw": bool(at.get("teamIsWinner")),
        })
    return out

def compute_podium(games):
    """Iz tekem izlušči podij (zmagovalec finala 1., poraženec 2., zmagovalec za 3. mesto 3.)."""
    podium = []
    fin = next((g for g in games if g["round"] == "F"), None)
    if fin and fin["hs"] is not None:
        win = fin["home"] if fin["hw"] else fin["away"]
        lose = fin["away"] if fin["hw"] else fin["home"]
        podium = [win, lose]
        third = next((g for g in games if g["round"] == "MSF"), None)
        if third and third["hs"] is not None:
            w3 = third["home"] if third["hw"] else third["away"]
            podium.append(w3)
    return podium

def main():
    # naloži obstoječe (za inkrementalno primerjavo)
    existing = {"season": SEASON, "tournaments": []}
    if os.path.exists(OUT):
        try:
            existing = json.load(open(OUT, encoding="utf-8"))
        except Exception:
            pass
    existing_by_event = {t.get("eventId"): t for t in existing.get("tournaments", [])}

    result_tours = []
    changed = False

    for cfg in TOURNAMENTS:
        ev, cat = cfg["eventId"], cfg["openCategoryId"]
        print(f"» {cfg['name']} ({ev[:8]}…)")

        teams_raw = fetch_json(f"{FIBA}/api/v2/category/{cat}/teams")
        games_raw = fetch_json(f"{FIBA}/api/v2/event/{ev}/games")

        prev = existing_by_event.get(ev, {})

        # če fetch ne uspe, OBDRŽI obstoječe (ne prepiši s praznim)
        if teams_raw is None and games_raw is None:
            print("  ! ni podatkov (blokada/napaka) — obdržim obstoječe")
            if prev:
                result_tours.append(prev)
            continue

        teams = parse_teams(teams_raw) if teams_raw else prev.get("teams", [])
        games = parse_games(games_raw, cat) if games_raw else prev.get("games", [])
        podium = compute_podium(games)
        status = "completed" if any(g["hs"] is not None for g in games) and \
                 any(g["round"] == "F" for g in games) else \
                 ("ongoing" if any(g["hs"] is not None for g in games) else "upcoming")

        tour = {
            "name": cfg["name"], "eventId": ev, "categoryId": cat,
            "date": cfg.get("date"), "status": status,
            "podium": podium, "teams": teams, "games": games,
        }
        result_tours.append(tour)

        # zaznaj spremembo
        if json.dumps(prev, sort_keys=True, ensure_ascii=False) != \
           json.dumps(tour, sort_keys=True, ensure_ascii=False):
            changed = True
            done = sum(1 for g in games if g["hs"] is not None)
            print(f"  ✓ posodobljeno: {len(teams)} ekip, {done}/{len(games)} odigranih, status={status}")
        else:
            print("  = brez sprememb")

    out_obj = {"season": SEASON, "tournaments": result_tours}
    if changed or not os.path.exists(OUT):
        os.makedirs(os.path.dirname(OUT), exist_ok=True)
        json.dump(out_obj, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print(f"\n💾 zapisano v {OUT}")
    else:
        print("\n= ni sprememb, datoteka nedotaknjena")

if __name__ == "__main__":
    main()
