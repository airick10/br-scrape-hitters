#!/usr/bin/env python3
"""
Pitchers-only Baseball-Reference scraper:
- Scrapes Standard Pitching "162 Game Avg" (even if table is in HTML comments)
- Adds bio fields: FirstName, LastName, Nickname, Height, Weight_lb, Bats, Throws, HS_City, HS_State, HS_Country
- Writes:
    - pitchers_162_raw.csv
    - pitchers_162_rounded.csv  (counting stats ceiled; IP/rates unchanged)
- Prints debug info so you can see footer labels per table
"""

import csv, re, time, math, random, os, pathlib
from typing import Dict, Optional, Tuple, List
import requests
from bs4 import BeautifulSoup, Comment

# ---------------- Config ----------------
URLS = [
    "https://www.baseball-reference.com/players/a/adamsba01.shtml",
"https://www.baseball-reference.com/players/a/alcansa01.shtml",
"https://www.baseball-reference.com/players/a/alexape01.shtml"
]

# A very "browser-like" UA (this matched the successful debug run)
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) EricBR/Pitch162-Working Safari/537.36"
TIMEOUT = 25
DELAY = (28, 45)   # be very polite; BR will throttle otherwise
DEBUG = True       # keep True until you confirm it’s steady

# Desired output order (extras appended later)
PREFERRED = [
    "W","L","W-L%","ERA",
    "G","GS","GF","CG","SHO","SV","IP",
    "H","R","ER","HR","BB","IBB","SO","HBP","BK","WP","BF",
    "ERA+","FIP","WHIP","H9","HR9","BB9","SO9","SO/BB"
]

# Counting vs rate (rounding)
COUNTING = {"W","L","G","GS","GF","CG","SHO","SV","H","R","ER","HR","BB","IBB","SO","HBP","BF","WP","BK"}
SKIP_ROUND = {"IP"}

# Map Baseball-Reference keys -> friendly
KEY_MAP = {
    "w":"W","l":"L","wl_perc":"W-L%","win_loss_perc":"W-L%","earned_run_avg":"ERA",
    "g":"G","gs":"GS","gf":"GF","cg":"CG","sho":"SHO","sv":"SV","ip":"IP",
    "h":"H","r":"R","er":"ER","hr":"HR","bb":"BB","ibb":"IBB","so":"SO",
    "hbp":"HBP","bk":"BK","wp":"WP","bf":"BF","batters_faced":"BF",
    "era_plus":"ERA+","fip":"FIP","whip":"WHIP",
    "hits_per_nine":"H9","home_runs_per_nine":"HR9","bases_on_balls_per_nine":"BB9",
    "strikeouts_per_nine":"SO9","strikeouts_per_base_on_balls":"SO/BB",
}

# Non-stat header cells to drop when building alignment
STAT_HEADER_DROP = {"season","year_id","age","team_id","tm","lg","lg_id","stint","pos","pos_summary"}

# Name & bio helpers
SUFFIXES = {"Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV", "V"}
US_STATE_CODES = {"AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC","PR","GU","VI","AS","MP"}

session = requests.Session()
session.headers.update({
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.7",
    "Connection": "keep-alive",
})

# ---------------- HTTP ----------------
def fetch_html(url: str) -> Optional[str]:
    try:
        r = session.get(url, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.text
    except requests.RequestException:
        return None
    return None

# ---------------- Utilities ----------------
def nlabel(s: str) -> str:
    s = (s or "").replace("\xa0"," ")
    s = re.sub(r'[.\u200b]+', '', s)
    s = re.sub(r'\s+', ' ', s).strip().lower()
    return s

def iter_all_tables(html: str):
    soup = BeautifulSoup(html, "html.parser")
    for t in soup.find_all("table"):
        yield t
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        if "<table" in c:
            try:
                csoup = BeautifulSoup(c, "html.parser")
                for t in csoup.find_all("table"):
                    yield t
            except Exception:
                continue

def build_header_stat_keys(table) -> List[str]:
    thead = table.find("thead")
    if not thead: return []
    rows = thead.find_all("tr")
    if not rows: return []
    hdr = rows[-1]
    keys = []
    for th in hdr.find_all(["th","td"]):
        k = (th.get("data-stat") or "").strip().lower()
        if not k:
            k = th.get_text(strip=True).strip().lower()
        k = k.replace(" ","_").replace("%","perc")
        if not k or k in STAT_HEADER_DROP:
            continue
        keys.append(k)
    return keys

# ---------------- Bio parsing ----------------
def get_display_name(page_html: str) -> str:
    soup = BeautifulSoup(page_html, "html.parser")
    h1 = soup.find("h1", attrs={"itemprop": "name"}) or soup.find("h1")
    return h1.get_text(strip=True) if h1 else ""

def split_first_last_with_suffix(full_name: str) -> Tuple[str, str]:
    parts = full_name.strip().split()
    if not parts: return "", ""
    if len(parts) == 1: return parts[0], ""
    if parts[-1] in SUFFIXES and len(parts) >= 3:
        return " ".join(parts[:-2]), parts[-2] + " " + parts[-1]
    return " ".join(parts[:-1]), parts[-1]

def parse_nickname(meta_soup: BeautifulSoup) -> str:
    for p in meta_soup.find_all("p"):
        strong = p.find("strong")
        if not strong: continue
        if strong.get_text(strip=True) in ("Nickname:", "Nicknames:"):
            raw = p.get_text(" ", strip=True)
            raw = re.sub(r'^(Nickname|Nicknames):\s*', '', raw, flags=re.I)
            for sep in ["•", ";", ",", " / ", " | "]:
                if sep in raw:
                    raw = raw.split(sep)[0]
                    break
            return raw.strip() if raw.strip() else "N/A"
    return "N/A"

def parse_height_weight_bats_throws(meta_soup: BeautifulSoup) -> Tuple[str, Optional[int], str, str]:
    height = ""
    weight_lb = None
    bats = ""
    throws = ""
    text_blob = " ".join(p.get_text(" ", strip=True) for p in meta_soup.find_all("p"))

    m_bt = re.search(r'Bats:\s*(Right|Left|Both)', text_blob, flags=re.I)
    if m_bt:
        bval = m_bt.group(1).title()
        bats = "Both" if bval.lower() == "both" else bval

    m_tr = re.search(r'Throws:\s*(Right|Left)', text_blob, flags=re.I)
    if m_tr:
        throws = m_tr.group(1).title()

    m_hw = re.search(r'(\d+)-(\d+)\s*,\s*(\d+)\s*lb', text_blob, flags=re.I)
    if m_hw:
        height = f"{m_hw.group(1)}'{m_hw.group(2)}"
        try:
            weight_lb = int(m_hw.group(3))
        except ValueError:
            weight_lb = None

    return height, weight_lb, bats, throws

def parse_high_school(meta_soup: BeautifulSoup) -> Tuple[str, str, str]:
    hs_p = None
    for p in meta_soup.find_all("p"):
        strong = p.find("strong")
        if strong and "High School" in strong.get_text():
            hs_p = p
            break
    if not hs_p: return "", "", ""
    txt = hs_p.get_text(" ", strip=True)
    parens = re.findall(r'\(([^)]+)\)', txt)
    loc = parens[-1] if parens else ""
    if not loc: return "", "", ""
    if "," in loc:
        city = loc.split(",")[0].strip()
        state = loc.split(",")[1].strip()
    else:
        city, state = loc.strip(), ""
    country = "USA" if state.upper() in US_STATE_CODES else ""
    return city, state, country

def parse_meta_bio(page_html: str) -> Dict[str, str]:
    soup = BeautifulSoup(page_html, "html.parser")
    meta = soup.find(id="meta")
    if not meta: return {}
    height, weight_lb, bats, throws = parse_height_weight_bats_throws(meta)
    hs_city, hs_state, hs_country = parse_high_school(meta)
    nickname = parse_nickname(meta)
    out = {"Nickname": nickname}
    if height: out["Height"] = height
    if weight_lb is not None: out["Weight_lb"] = str(weight_lb)
    if bats: out["Bats"] = bats
    if throws: out["Throws"] = throws
    if hs_city: out["HS_City"] = hs_city
    if hs_state: out["HS_State"] = hs_state
    if hs_country: out["HS_Country"] = hs_country
    return out

# ---------------- Pitching 162 finder (tolerant) ----------------
def find_pitching_162_row(html: str, url: str) -> Optional[Dict[str,str]]:
    found_any_pitch_table = 0
    saw_tfoot = 0
    footer_labels = []

    candidates = []
    for tbl in iter_all_tables(html):
        tid = (tbl.get("id") or "").lower()
        if not ("pitch" in tid):  # accept any id with 'pitch' (covers pitching_standard, etc.)
            continue
        found_any_pitch_table += 1

        stat_keys = build_header_stat_keys(tbl)
        tfoot = tbl.find("tfoot")
        if not tfoot:
            if DEBUG: print(f"  [debug] table id={tid}: NO <tfoot>")
            continue
        saw_tfoot += 1

        for tr in tfoot.find_all("tr"):
            th = tr.find("th")
            label = nlabel(th.get_text(strip=True) if th else "")
            if label: footer_labels.append(label)

            # accept if label contains both '162' and 'avg'
            if not (("162" in label) and ("avg" in label)):
                continue

            # Prefer per-cell data-stat; else align by header position
            cells = tr.find_all("td")
            row_raw, used_ds = {}, False
            for td in cells:
                k = (td.get("data-stat") or "").strip().lower().replace(" ","_")
                val = td.get_text(strip=True).replace(",","")
                if k:
                    used_ds = True
                    friendly = KEY_MAP.get(k, KEY_MAP.get(k.lower(), k))
                    row_raw[friendly] = val

            if not used_ds:
                for i, td in enumerate(cells):
                    if i >= len(stat_keys): break
                    raw = stat_keys[i]
                    friendly = KEY_MAP.get(raw, KEY_MAP.get(raw.lower(), raw))
                    row_raw[friendly] = td.get_text(strip=True).replace(",","")

            # prune empties and order
            row_raw = {k:v for k,v in row_raw.items() if k and v != ""}
            ordered = {}
            for k in PREFERRED:
                if k in row_raw: ordered[k] = row_raw[k]
            for k,v in row_raw.items():
                if k not in ordered: ordered[k] = v

            candidates.append((0 if tid=="pitching_standard" else 1, ordered))

    if DEBUG:
        print(f"  [debug] url={url}")
        print(f"  [debug] pitching tables seen: {found_any_pitch_table}, with tfoot: {saw_tfoot}")
        if footer_labels:
            print("  [debug] footer labels found:")
            for lab in sorted(set(footer_labels)):
                print(f"    - '{lab}'")
        else:
            print("  [debug] no footer labels found in pitching tables.")

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]

# ---------------- Rounding & CSV ----------------
def ceil_counting(d: Dict[str,str]) -> Dict[str,str]:
    for k, v in list(d.items()):
        if k in COUNTING and k not in SKIP_ROUND and v not in ("", None):
            s = str(v).replace(",","").strip()
            if re.match(r'^-?\d+(\.\d+)?$', s):
                try: d[k] = str(math.ceil(float(s)))
                except: pass
    return d

def write_csv(path: str, header: List[str], rows: List[Dict[str,str]]):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})

def round_counting_columns_in_csv(input_path: str, output_path: str):
    with open(input_path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        header = reader.fieldnames or []
    cols = [c for c in header if c in COUNTING and c not in SKIP_ROUND]
    for r in rows:
        for c in cols:
            v = r.get(c, "")
            if not v: continue
            s = str(v).replace(",","").strip()
            if re.match(r'^-?\d+(\.\d+)?$', s):
                try: r[c] = str(math.ceil(float(s)))
                except: pass
    write_csv(output_path, header, rows)

# ---------------- Main ----------------
def main():
    out_rows = []
    all_fields = set()
    if DEBUG:
        pathlib.Path("br_debug_html").mkdir(exist_ok=True)

    for url in URLS:
        print(f"\nProcessing: {url}")
        html = fetch_html(url)

        rec: Dict[str, str] = {
            "FirstName": "", "LastName": "", "Nickname": "N/A",
            "Height": "", "Weight_lb": "", "Bats": "", "Throws": "",
            "HS_City": "", "HS_State": "", "HS_Country": "",
            "BaseballReferenceURL": url, "error": "",
        }

        if not html:
            rec["error"] = "fetch_failed"
            out_rows.append(rec)
            time.sleep(random.uniform(*DELAY)); continue

        # Save raw HTML (debug)
        if DEBUG:
            safe = re.sub(r'[^a-z0-9]+', '_', url.lower())
            with open(os.path.join("br_debug_html", f"{safe}.html"), "w") as f:
                f.write(html)

        # Bio fields
        display_name = get_display_name(html)
        first, last = split_first_last_with_suffix(display_name)
        rec["FirstName"], rec["LastName"] = first, last
        rec.update(parse_meta_bio(html))

        # Pitching 162 row
        row = find_pitching_162_row(html, url)
        if row:
            rec.update(ceil_counting(row))
            all_fields.update(row.keys())
        else:
            rec["error"] = "162_row_not_found"

        out_rows.append(rec)
        time.sleep(random.uniform(*DELAY))

    # Build header: bio + preferred stats + extras
    header = [
        "FirstName","LastName","Nickname",
        "Height","Weight_lb","Bats","Throws",
        "HS_City","HS_State","HS_Country",
        "BaseballReferenceURL","error"
    ]
    for c in PREFERRED:
        if c in all_fields and c not in header:
            header.append(c)
    extras = sorted([c for c in (all_fields - set(header))])
    header += extras

    raw_csv = "pitchers_162_raw.csv"
    write_csv(raw_csv, header, out_rows)
    print("\nSaved:", raw_csv)

    rounded_csv = "pitchers_162_rounded.csv"
    round_counting_columns_in_csv(raw_csv, rounded_csv)
    print("Saved:", rounded_csv)

if __name__ == "__main__":
    main()