#!/usr/bin/env python3
import csv
import re
import time
import math
import random
from typing import Dict, Optional, Tuple, List
import requests
from bs4 import BeautifulSoup, Comment

# ------------------------ Config ------------------------

URLS = [
    "https://www.baseball-reference.com/players/a/alomasa02.shtml",
"https://www.baseball-reference.com/players/a/ausmubr01.shtml",
"https://www.baseball-reference.com/players/b/baileed01.shtml"
]

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X) EricBR/2.2"
TIMEOUT = 25
DELAY = 1.5

PREFERRED_BATTING = [
    "G","PA","AB","R","H","2B","3B","HR","RBI","SB","CS","BB","SO",
    "BA","OBP","SLG","OPS","OPS_plus","TB","GIDP","HBP","SH","SF","IBB"
]

COUNTING_STATS = {"G","PA","AB","R","H","2B","3B","HR","RBI","SB","CS","BB","SO","TB","GIDP","HBP","SH","SF","IBB"}
RATE_STATS = {"BA","OBP","SLG","OPS","OPS_plus"}

# Optional alt naming scheme you mentioned before; we'll round these too if present
B_PREFIX_COUNTING = {
    "b_ab","b_bb","b_cs","b_doubles","b_games","b_gidp","b_h","b_hbp","b_hr",
    "b_ibb","b_pa","b_r","b_rbi","b_sb","b_sf","b_so","b_tb","b_triples"
}

US_STATE_CODES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY",
    "LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND",
    "OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "DC","PR","GU","VI","AS","MP"
}
SUFFIXES = {"Jr.", "Jr", "Sr.", "Sr", "II", "III", "IV", "V"}

session = requests.Session()
session.headers.update({"User-Agent": UA})

# ------------------------ HTTP -------------------------

def fetch_html(url: str) -> Optional[str]:
    try:
        r = session.get(url, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.text
    except requests.RequestException:
        pass
    return None

# ------------------------ Name parsing -----------------

def get_display_name(page_html: str) -> str:
    soup = BeautifulSoup(page_html, "html.parser")
    h1 = soup.find("h1", attrs={"itemprop": "name"}) or soup.find("h1")
    return h1.get_text(strip=True) if h1 else ""

def split_first_last_with_suffix(full_name: str) -> Tuple[str, str]:
    parts = full_name.strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    if parts[-1] in SUFFIXES and len(parts) >= 3:
        first = " ".join(parts[:-2])
        last = parts[-2] + " " + parts[-1]
        return first, last
    return " ".join(parts[:-1]), parts[-1]

# ------------------------ Bio parsing ------------------

def parse_nickname(meta_soup: BeautifulSoup) -> str:
    for p in meta_soup.find_all("p"):
        strong = p.find("strong")
        if not strong:
            continue
        label = strong.get_text(strip=True)
        if label in ("Nickname:", "Nicknames:"):
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
        bats_val = m_bt.group(1).title()
        bats = "Both" if bats_val.lower() == "both" else bats_val

    m_tr = re.search(r'Throws:\s*(Right|Left)', text_blob, flags=re.I)
    if m_tr:
        throws = m_tr.group(1).title()

    m_hw = re.search(r'(\d+)-(\d+)\s*,\s*(\d+)\s*lb', text_blob, flags=re.I)
    if m_hw:
        height = f"{m_hw.group(1)}'{m_hw.group(2)}"  # e.g., 6'2
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
    if not hs_p:
        return "", "", ""
    txt = hs_p.get_text(" ", strip=True)
    parens = re.findall(r'\(([^)]+)\)', txt)
    loc = parens[-1] if parens else ""
    if not loc:
        return "", "", ""
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
    if not meta:
        return {}
    height, weight_lb, bats, throws = parse_height_weight_bats_throws(meta)
    hs_city, hs_state, hs_country = parse_high_school(meta)
    nickname = parse_nickname(meta)

    out: Dict[str, str] = {"Nickname": nickname}
    if height:
        out["Height"] = height
    if weight_lb is not None:
        out["Weight_lb"] = str(weight_lb)
    if bats:
        out["Bats"] = bats
    if throws:
        out["Throws"] = throws
    if hs_city:
        out["HS_City"] = hs_city
    if hs_state:
        out["HS_State"] = hs_state
    if hs_country:
        out["HS_Country"] = hs_country
    return out

# ------------------------ 162-game logic ----------------

def iter_all_tables(page_html: str):
    soup = BeautifulSoup(page_html, "html.parser")
    for tbl in soup.find_all("table"):
        yield tbl
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        if any(token in c for token in ("table", "tbody", "tfoot", "thead")):
            try:
                csoup = BeautifulSoup(c, "html.parser")
                for tbl in csoup.find_all("table"):
                    yield tbl
            except Exception:
                continue

def pick_batting_standard_table(tables: List[BeautifulSoup]) -> List[BeautifulSoup]:
    candidates = []
    for t in tables:
        tid = (t.get("id") or "").lower()
        if tid == "batting_standard":
            candidates.insert(0, t)
        else:
            candidates.append(t)
    return candidates

def find_row_by_footer_162(table) -> Optional[Dict[str, str]]:
    tfoot = table.find("tfoot")
    if not tfoot:
        return None
    for tr in tfoot.find_all("tr"):
        th = tr.find("th")
        label = th.get_text(strip=True) if th else ""
        if label == "162 Game Avg" or "162" in label:
            row = {}
            for td in tr.find_all("td"):
                stat = td.get("data-stat") or ""
                val = td.get_text(strip=True).replace(",", "")
                if stat:
                    row[stat] = val
            return row if row else None
    return None

def get_career_totals_row(table) -> Optional[Tuple[Dict[str,str], int]]:
    tfoot = table.find("tfoot")
    if not tfoot:
        return None
    candidates = []
    for tr in tfoot.find_all("tr"):
        th = tr.find("th")
        label = th.get_text(strip=True) if th else ""
        if label in {"MLB", "Career"} or "Yrs" in label:
            data = {}
            for td in tr.find_all("td"):
                stat = td.get("data-stat") or ""
                val = td.get_text(strip=True).replace(",", "")
                if stat:
                    data[stat] = val
            if data:
                try:
                    g_total = int(float(data.get("G", "0")))
                except ValueError:
                    g_total = None
                candidates.append((data, g_total))
    if not candidates:
        trs = tfoot.find_all("tr")
        if trs:
            last = trs[-1]
            data = {}
            for td in last.find_all("td"):
                stat = td.get("data-stat") or ""
                val = td.get_text(strip=True).replace(",", "")
                if stat:
                    data[stat] = val
            if data:
                try:
                    g_total = int(float(data.get("G", "0")))
                except ValueError:
                    g_total = None
                candidates.append((data, g_total))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[1] or 0), reverse=True)
    return candidates[0]

def compute_162_from_totals(totals: Dict[str,str]) -> Optional[Dict[str,str]]:
    try:
        g_total = float(totals.get("G", "0"))
        if g_total <= 0:
            return None
    except ValueError:
        return None
    scale = 162.0 / g_total
    out: Dict[str,str] = {}
    for k in COUNTING_STATS:
        v = totals.get(k)
        if not v:
            continue
        try:
            out[k] = str(round(float(v) * scale, 1))
        except ValueError:
            pass
    for k in RATE_STATS:
        if k in totals and totals[k] != "":
            out[k] = totals[k]
    return out if out else None

def round_up_counting(stats: Dict[str, str]) -> Dict[str, str]:
    for k in COUNTING_STATS:
        if k in stats and stats[k] not in ("", None):
            try:
                val_str = str(stats[k]).replace(",", "").strip()
                if re.match(r'^-?\d+(\.\d+)?$', val_str):
                    stats[k] = str(math.ceil(float(val_str)))
            except Exception:
                pass
    return stats

def extract_player_162(page_html: str) -> Tuple[Optional[Dict[str,str]], str]:
    tables = list(iter_all_tables(page_html))
    for tbl in pick_batting_standard_table(tables):
        row = find_row_by_footer_162(tbl)
        if row:
            return (round_up_counting(row), "")
        totals = get_career_totals_row(tbl)
        if totals:
            computed = compute_162_from_totals(totals[0])
            if computed:
                return (round_up_counting(computed), "computed_from_totals")
    return (None, "not_found")

# ------------------------ Output rounding pass -------------------------

def round_counting_columns_in_csv(input_path: str, output_path: str):
    """
    Post-process CSV:
      - If 'b_*' counting columns exist, ceil them.
      - Else, ceil standard BR counting columns.
    Writes output_path.
    """
    # Load
    with open(input_path, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    # Decide which columns to round
    b_cols_present = [c for c in fieldnames if c in B_PREFIX_COUNTING]
    if b_cols_present:
        cols_to_round = b_cols_present
    else:
        cols_to_round = [c for c in fieldnames if c in COUNTING_STATS]

    # Round
    for r in rows:
        for c in cols_to_round:
            v = r.get(c, "")
            if v is None or str(v).strip() == "":
                continue
            s = str(v).replace(",", "").strip()
            if re.match(r'^-?\d+(\.\d+)?$', s):
                try:
                    r[c] = str(math.ceil(float(s)))
                except ValueError:
                    pass

    # Write
    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

# ------------------------ Main -------------------------

def main():
    rows = []
    all_fields = set()

    for url in URLS:
        print(f"Processing: {url}")
        html = fetch_html(url)

        rec: Dict[str, str] = {
            "FirstName": "",
            "LastName": "",
            "Nickname": "N/A",
            "Height": "",
            "Weight_lb": "",
            "Bats": "",
            "Throws": "",
            "HS_City": "",
            "HS_State": "",
            "HS_Country": "",
            "BaseballReferenceURL": url,
            "error": "",
        }

        if not html:
            rec["error"] = "fetch_failed"
            rows.append(rec)
            continue

        display_name = get_display_name(html)
        first, last = split_first_last_with_suffix(display_name)
        rec["FirstName"] = first
        rec["LastName"] = last

        stats, err = extract_player_162(html)
        if stats:
            rec.update(stats)
            rec["error"] = err or ""
            all_fields.update(stats.keys())
        else:
            rec["error"] = err or "unknown"

        bio = parse_meta_bio(html)
        rec.update(bio)
        all_fields.update(bio.keys())

        rows.append(rec)
        time.sleep(random.uniform(15, 25))  # be polite

    header = [
        "FirstName","LastName","Nickname",
        "Height","Weight_lb","Bats","Throws",
        "HS_City","HS_State","HS_Country",
        "BaseballReferenceURL","error"
    ]
    header += [c for c in PREFERRED_BATTING if c in all_fields]
    extras = sorted([c for c in (all_fields - set(header))])
    header += extras

    raw_csv = "162_game_avg.csv"
    with open(raw_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in header})

    print(f"Saved: {raw_csv}")

    # --- Post-process rounding into cleaner file ---
    rounded_csv = "162_game_avg_rounded.csv"
    round_counting_columns_in_csv(raw_csv, rounded_csv)
    print(f"Saved rounded file: {rounded_csv}")

if __name__ == "__main__":
    main()
