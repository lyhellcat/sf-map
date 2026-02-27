#!/usr/bin/env python3
"""Build local SF Hong Kong store data for map rendering.

Data sources:
1) Chinese store page (required): base store list + business hours.
2) English store page (optional): English addresses + business hours.
3) Official HK store API (optional): precise coordinates + EN metadata.

Coordinate priority:
1) Official API coordinates.
2) District-center fallback with deterministic jitter (offline-safe).
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

SOURCE_URL_ZH = "https://htm.sf-express.com/hk/sc/dynamic_function/S.F.Network/SF_store_address/"
SOURCE_URL_EN = "https://htm.sf-express.com/hk/en/dynamic_function/S.F.Network/SF_store_address/"
OFFICIAL_STORE_URL = "https://htm.sf-express.com/sf-service-core-web/service/store/hk/query"

ROOT = Path(__file__).resolve().parents[1]
SOURCE_HTML_ZH = ROOT / "sf_store_address.html"
SOURCE_HTML_EN = ROOT / "sf_store_address_en.html"
DATA_DIR = ROOT / "data"

OFFICIAL_RAW_JSON = DATA_DIR / "sf_hk_store_official_raw.json"
STORES_JSON = DATA_DIR / "sf_hk_stores.json"
STORES_GEOJSON = DATA_DIR / "sf_hk_stores.geojson"
FALLBACK_JSON = DATA_DIR / "sf_hk_fallback_rows.json"

USER_AGENT = "sf-hk-map-local-demo/1.0 (local use; no commercial traffic)"

COUNCIL_CENTERS = {
    "中西区": (22.2866, 114.1543),
    "东区": (22.2840, 114.2240),
    "湾仔区": (22.2765, 114.1753),
    "南区": (22.2473, 114.1588),
    "油尖旺区": (22.3121, 114.1696),
    "深水埗区": (22.3307, 114.1622),
    "九龙城区": (22.3286, 114.1914),
    "黄大仙区": (22.3431, 114.1938),
    "观塘区": (22.3133, 114.2250),
    "葵青区": (22.3568, 114.1272),
    "荃湾区": (22.3711, 114.1143),
    "屯门区": (22.3919, 113.9772),
    "元朗区": (22.4456, 114.0222),
    "北区": (22.4957, 114.1274),
    "大埔区": (22.4502, 114.1687),
    "沙田区": (22.3835, 114.1880),
    "西贡区": (22.3837, 114.2700),
    "离岛区": (22.2802, 113.9409),
}

# Manual EN patch for rows absent from upstream EN/official sources.
EN_ADDRESS_OVERRIDES = {
    "852QB": {
        "district_en": "Chek Lap Kok",
        "name_en": "Chek Lap Kok Airport SF Service Point",
        "address_en": "Counter 1-2, Area J, L7, Terminal 1, Hong Kong International Airport, Chek Lap Kok, Lantau Island, New Territories, Hong Kong",
    }
}


@dataclass
class Store:
    district: str
    code: str
    name: str
    address: str
    hours_weekday: str
    hours_sat: str
    hours_sun_holiday: str


def load_source_html(url: str, path: Path, refresh: bool, required: bool) -> str:
    if refresh or not path.exists():
        try:
            response = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
            response.raise_for_status()
            path.write_text(response.text, encoding="utf-8")
        except requests.RequestException:
            if required or not path.exists():
                if required:
                    raise
                return ""
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")


def extract_hk_section(page_html: str) -> str:
    start_match = re.search(r"<h3[^>]*>.*?香港岛.*?</h3>", page_html, flags=re.IGNORECASE | re.DOTALL)
    if not start_match:
        raise RuntimeError("Cannot find Hong Kong section start (香港岛).")

    tail = page_html[start_match.start() :]
    end_match = re.search(r"<span[^>]*>\s*澳门\s*</span>|<strong>\s*澳门\s*</strong>", tail, flags=re.IGNORECASE)
    if not end_match:
        raise RuntimeError("Cannot find Hong Kong section end (澳门).")

    return tail[: end_match.start()]


def strip_tags(fragment: str) -> str:
    fragment = re.sub(r"<br\s*/?>", " ", fragment, flags=re.IGNORECASE)
    fragment = re.sub(r"<[^>]+>", " ", fragment)
    fragment = html.unescape(fragment)
    fragment = fragment.replace("\xa0", " ")
    fragment = re.sub(r"\s+", " ", fragment)
    return fragment.strip()


def is_store_code(text: str) -> bool:
    return re.fullmatch(r"852[A-Z0-9]+", text or "") is not None


def clean_address(text: str) -> str:
    text = re.sub(r"\^852[A-Z0-9]+\^", "", text)
    text = text.replace("^", "")
    text = text.replace("*", "")
    text = text.replace("Â", "")
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ,;；")


def normalize_time_text(text: str) -> str:
    text = text.strip()
    text = re.sub(r"\s*[-~]\s*", "-", text)
    text = text.replace("–", "-")
    text = text.replace("—", "-")
    return text


def looks_like_time(text: str) -> bool:
    s = normalize_time_text(text)
    if not s:
        return False
    if re.search(r"\d{1,2}:\d{2}\s*[-]\s*\d{1,2}:\d{2}", s):
        return True
    if s.upper() in {"OFF", "CLOSED"}:
        return True
    if s in {"休息", "關閉", "关闭"}:
        return True
    return False


def looks_like_en_address(text: str) -> bool:
    if not text:
        return False
    s = text.strip()
    if looks_like_time(s):
        return False
    tokens = [
        "Hong Kong",
        "Kowloon",
        "New Territories",
        "Lantau",
        "Chek Lap Kok",
        "Street",
        "Road",
        "Building",
        "Shop",
        "G/F",
        "Floor",
    ]
    return any(tok in s for tok in tokens)


def pick_hours_from_cells(cells: List[str]) -> Tuple[str, str, str]:
    times = [normalize_time_text(c) for c in cells if looks_like_time(c)]
    if len(times) >= 3:
        return times[0], times[1], times[2]
    if len(times) == 2:
        return times[0], times[1], times[1]
    if len(times) == 1:
        return times[0], "", ""
    return "", "", ""


def parse_stores_zh(hk_section_html: str) -> List[Store]:
    row_pattern = re.compile(r"<tr[^>]*>(.*?)</tr>", flags=re.IGNORECASE | re.DOTALL)
    cell_pattern = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", flags=re.IGNORECASE | re.DOTALL)

    stores: List[Store] = []
    seen_codes = set()
    last_district = ""

    for row_html in row_pattern.findall(hk_section_html):
        raw_cells = cell_pattern.findall(row_html)
        if len(raw_cells) < 4:
            continue

        cells = [strip_tags(cell) for cell in raw_cells]
        if not any(cells):
            continue

        joined = " ".join(cells)
        if "点码" in joined and "地区" in joined:
            continue

        code_idx = next((i for i, cell in enumerate(cells) if is_store_code(cell)), -1)
        if code_idx < 0:
            continue

        code = cells[code_idx]
        if code in seen_codes:
            continue

        district = ""
        for cell in cells[:code_idx]:
            if cell and not is_store_code(cell):
                district = cell
                break
        if not district:
            district = last_district
        if district:
            last_district = district

        post_cells = [c for c in cells[code_idx + 1 :] if c]
        if len(post_cells) < 2:
            continue

        name = post_cells[0]
        address = ""
        for cell in post_cells:
            if "香港" in cell or "赤鱲角" in cell or "大屿山" in cell:
                address = cell
                break
        if not address and len(post_cells) > 1:
            address = post_cells[1]

        address = clean_address(address)
        hours_weekday, hours_sat, hours_sun_holiday = pick_hours_from_cells(post_cells)

        if not district or not name or not address:
            continue
        if "澳门" in district or "澳门" in address:
            continue

        seen_codes.add(code)
        stores.append(
            Store(
                district=district,
                code=code,
                name=name,
                address=address,
                hours_weekday=hours_weekday,
                hours_sat=hours_sat,
                hours_sun_holiday=hours_sun_holiday,
            )
        )

    return stores


def _prefer_name_en(a: str, b: str) -> str:
    # Prefer non-address short label as station name.
    def score(v: str) -> int:
        if not v:
            return 0
        if looks_like_time(v):
            return 1
        if looks_like_en_address(v):
            return 2
        return 3

    sa, sb = score(a), score(b)
    if sb > sa:
        return b
    return a


def _prefer_address_en(a: str, b: str) -> str:
    # Prefer address-like value over anything else.
    if looks_like_en_address(a):
        return a
    if looks_like_en_address(b):
        return b
    if a and not looks_like_time(a):
        return a
    if b and not looks_like_time(b):
        return b
    return a or b


def parse_english_map(page_html: str) -> Dict[str, Dict[str, str]]:
    if not page_html:
        return {}

    row_pattern = re.compile(r"<tr[^>]*>(.*?)</tr>", flags=re.IGNORECASE | re.DOTALL)
    cell_pattern = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", flags=re.IGNORECASE | re.DOTALL)

    out: Dict[str, Dict[str, str]] = {}
    last_district = ""

    for row_html in row_pattern.findall(page_html):
        raw_cells = cell_pattern.findall(row_html)
        if len(raw_cells) < 4:
            continue

        cells = [strip_tags(cell) for cell in raw_cells]
        if not any(cells):
            continue

        code_idx = next((i for i, cell in enumerate(cells) if is_store_code(cell)), -1)
        if code_idx < 0:
            continue

        code = cells[code_idx]

        district = ""
        for cell in cells[:code_idx]:
            if cell and not is_store_code(cell):
                district = cell
                break
        if not district:
            district = last_district
        if district:
            last_district = district

        post_cells = [c for c in cells[code_idx + 1 :] if c]
        if not post_cells:
            continue

        address_en = ""
        for cell in post_cells:
            if looks_like_en_address(cell):
                address_en = clean_address(cell)
                break

        name_en = ""
        for cell in post_cells:
            s = cell.strip()
            if not s or looks_like_time(s) or looks_like_en_address(s):
                continue
            name_en = s
            break

        hours_weekday_en, hours_sat_en, hours_sun_holiday_en = pick_hours_from_cells(post_cells)

        current = out.get(code, {})
        out[code] = {
            "district_en": current.get("district_en") or district,
            "name_en": _prefer_name_en(str(current.get("name_en") or ""), name_en),
            "address_en": _prefer_address_en(str(current.get("address_en") or ""), address_en),
            "hours_weekday_en": current.get("hours_weekday_en") or hours_weekday_en,
            "hours_sat_en": current.get("hours_sat_en") or hours_sat_en,
            "hours_sun_holiday_en": current.get("hours_sun_holiday_en") or hours_sun_holiday_en,
        }

    return out


def parse_service_time_cn(service_time: str) -> Tuple[str, str, str]:
    if not service_time:
        return "", "", ""

    weekday = ""
    sat = ""
    sun_holiday = ""

    parts = [p.strip() for p in service_time.split(";") if p.strip()]
    ordered_values: List[str] = []

    for part in parts:
        if "," in part:
            label, value = part.split(",", 1)
        else:
            label, value = part, ""
        label = label.strip()
        value = normalize_time_text(value.strip())
        if not value:
            m = re.search(r"(\d{1,2}:\d{2}\s*[-]\s*\d{1,2}:\d{2}|OFF|休息)", label, flags=re.IGNORECASE)
            value = normalize_time_text(m.group(1)) if m else ""

        if value:
            ordered_values.append(value)

        if "周一至周五" in label or "星期一至五" in label:
            weekday = value or weekday
        elif "周六" in label and ("周日" in label or "公眾假期" in label or "公众假期" in label):
            sat = value or sat
            sun_holiday = value or sun_holiday
        elif "周六" in label or "星期六" in label:
            sat = value or sat
        elif "周日" in label or "星期日" in label or "公眾假期" in label or "公众假期" in label:
            sun_holiday = value or sun_holiday

    if not weekday and len(ordered_values) >= 1:
        weekday = ordered_values[0]
    if not sat and len(ordered_values) >= 2:
        sat = ordered_values[1]
    if not sun_holiday and len(ordered_values) >= 3:
        sun_holiday = ordered_values[2]
    if not sun_holiday and sat and len(ordered_values) == 2:
        sun_holiday = sat

    return weekday, sat, sun_holiday


def parse_service_time_en(service_time_en: str) -> Tuple[str, str, str]:
    if not service_time_en:
        return "", "", ""

    weekday = ""
    sat = ""
    sun_holiday = ""

    parts = [p.strip() for p in service_time_en.split(";") if p.strip()]
    ordered_values: List[str] = []

    for part in parts:
        part_norm = part.replace("&", "and")
        m = re.search(r"(\d{1,2}:\d{2}\s*[-]\s*\d{1,2}:\d{2}|OFF|Closed)", part_norm, flags=re.IGNORECASE)
        value = normalize_time_text(m.group(1)) if m else ""
        if value:
            ordered_values.append(value)

        label = part_norm.lower()
        if "mon" in label and "fri" in label:
            weekday = value or weekday
        elif "sat" in label and "sun" in label:
            sat = value or sat
            sun_holiday = value or sun_holiday
        elif "sat" in label:
            sat = value or sat
        elif "sun" in label or "public holiday" in label:
            sun_holiday = value or sun_holiday

    if not weekday and len(ordered_values) >= 1:
        weekday = ordered_values[0]
    if not sat and len(ordered_values) >= 2:
        sat = ordered_values[1]
    if not sun_holiday and len(ordered_values) >= 3:
        sun_holiday = ordered_values[2]
    if not sun_holiday and sat and len(ordered_values) == 2:
        sun_holiday = sat

    return weekday, sat, sun_holiday


def parse_official_store_map(payload: Dict[str, object]) -> Dict[str, Dict[str, object]]:
    result = payload.get("result")
    if not isinstance(result, list):
        return {}

    out: Dict[str, Dict[str, object]] = {}

    for row in result:
        if not isinstance(row, dict):
            continue

        code = str(row.get("deptCode") or "").strip().upper()
        if not is_store_code(code):
            continue

        lat = row.get("lat")
        lng = row.get("lng")

        hours_weekday, hours_sat, hours_sun_holiday = parse_service_time_cn(str(row.get("serviceTime") or ""))
        hours_weekday_en, hours_sat_en, hours_sun_holiday_en = parse_service_time_en(str(row.get("serviceTimeEn") or ""))

        candidate = {
            "lat": float(lat) if isinstance(lat, (int, float)) else None,
            "lon": float(lng) if isinstance(lng, (int, float)) else None,
            "name_en": str(row.get("nameEn") or "").strip(),
            "address_en": clean_address(str(row.get("addressEn") or "")),
            "district_en": str(row.get("districtEn") or "").strip(),
            "hours_weekday": hours_weekday,
            "hours_sat": hours_sat,
            "hours_sun_holiday": hours_sun_holiday,
            "hours_weekday_en": hours_weekday_en,
            "hours_sat_en": hours_sat_en,
            "hours_sun_holiday_en": hours_sun_holiday_en,
        }

        current = out.get(code)
        if not current:
            out[code] = candidate
            continue

        merged = dict(current)
        if merged.get("lat") is None and candidate.get("lat") is not None:
            merged["lat"] = candidate["lat"]
        if merged.get("lon") is None and candidate.get("lon") is not None:
            merged["lon"] = candidate["lon"]

        merged["district_en"] = merged.get("district_en") or candidate.get("district_en") or ""
        merged["name_en"] = _prefer_name_en(str(merged.get("name_en") or ""), str(candidate.get("name_en") or ""))
        merged["address_en"] = _prefer_address_en(str(merged.get("address_en") or ""), str(candidate.get("address_en") or ""))

        for k in [
            "hours_weekday",
            "hours_sat",
            "hours_sun_holiday",
            "hours_weekday_en",
            "hours_sat_en",
            "hours_sun_holiday_en",
        ]:
            merged[k] = merged.get(k) or candidate.get(k) or ""

        out[code] = merged

    return out


def fetch_official_store_map(timeout: int, skip_official_api: bool) -> Dict[str, Dict[str, object]]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not skip_official_api:
        try:
            response = requests.get(
                OFFICIAL_STORE_URL,
                params={
                    "storeType": "1|8|9|10|4|6",
                    "rateCode": "852",
                    "keyWord": "",
                },
                timeout=timeout,
                headers={"User-Agent": USER_AGENT},
            )
            response.raise_for_status()
            payload = response.json()
            OFFICIAL_RAW_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            mapping = parse_official_store_map(payload)
            if mapping:
                return mapping
        except (requests.RequestException, ValueError):
            pass

    if OFFICIAL_RAW_JSON.exists():
        try:
            payload = json.loads(OFFICIAL_RAW_JSON.read_text(encoding="utf-8"))
            return parse_official_store_map(payload)
        except json.JSONDecodeError:
            return {}

    return {}


def detect_council(address: str) -> Optional[str]:
    match = re.search(r"香港(?:香港岛|九龙|新界|离岛)?([^\s,，]{1,6}区)", address)
    if match:
        return match.group(1)
    return None


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def jitter_from_code(code: str) -> Tuple[float, float]:
    digest = hashlib.md5(code.encode("utf-8")).hexdigest()
    seed = int(digest[:16], 16)
    lat_offset = ((seed % 2001) - 1000) / 100000.0
    lon_offset = (((seed // 2001) % 2001) - 1000) / 100000.0
    return lat_offset, lon_offset


def first_non_empty(*vals: object) -> str:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def build_rows(
    stores: List[Store],
    english_map: Dict[str, Dict[str, str]],
    official_map: Dict[str, Dict[str, object]],
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]], int]:
    rows: List[Dict[str, object]] = []
    fallback_rows: List[Dict[str, object]] = []
    en_fallback_count = 0

    for store in stores:
        off = official_map.get(store.code, {})
        en = english_map.get(store.code, {})
        override = EN_ADDRESS_OVERRIDES.get(store.code, {})

        address_en = first_non_empty(
            override.get("address_en", ""),
            _prefer_address_en(str(off.get("address_en") or ""), str(en.get("address_en") or "")),
        )
        if not address_en:
            # Keep non-empty to satisfy UI/search. This should be rare.
            address_en = store.address
            en_fallback_count += 1

        name_en = first_non_empty(
            override.get("name_en", ""),
            off.get("name_en", ""),
        )
        district_en = first_non_empty(
            override.get("district_en", ""),
            off.get("district_en", ""),
            en.get("district_en", ""),
        )

        row = {
            "district": store.district,
            "district_en": district_en,
            "code": store.code,
            "name": store.name,
            "name_en": name_en,
            "address": store.address,
            "address_en": address_en,
            "hours_weekday": first_non_empty(store.hours_weekday, off.get("hours_weekday", "")),
            "hours_sat": first_non_empty(store.hours_sat, off.get("hours_sat", "")),
            "hours_sun_holiday": first_non_empty(store.hours_sun_holiday, off.get("hours_sun_holiday", "")),
            "hours_weekday_en": first_non_empty(off.get("hours_weekday_en", ""), en.get("hours_weekday_en", "")),
            "hours_sat_en": first_non_empty(off.get("hours_sat_en", ""), en.get("hours_sat_en", "")),
            "hours_sun_holiday_en": first_non_empty(off.get("hours_sun_holiday_en", ""), en.get("hours_sun_holiday_en", "")),
            "lat": None,
            "lon": None,
            "coord_source": None,
            "council": detect_council(store.address),
        }

        lat = off.get("lat")
        lon = off.get("lon")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            row["lat"] = float(lat)
            row["lon"] = float(lon)
            row["coord_source"] = "sf_api"
        else:
            council = row["council"]
            base = COUNCIL_CENTERS.get(council or "", (22.3193, 114.1694))
            lat_delta, lon_delta = jitter_from_code(store.code)
            row["lat"] = clamp(base[0] + lat_delta, 22.15, 22.58)
            row["lon"] = clamp(base[1] + lon_delta, 113.82, 114.45)
            row["coord_source"] = "district_fallback"
            fallback_rows.append(row)

        rows.append(row)

    return rows, fallback_rows, en_fallback_count


def write_outputs(rows: List[Dict[str, object]], fallback_rows: List[Dict[str, object]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    all_rows = sorted(rows, key=lambda x: x["code"])
    STORES_JSON.write_text(json.dumps(all_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    FALLBACK_JSON.write_text(json.dumps(fallback_rows, ensure_ascii=False, indent=2), encoding="utf-8")

    features = []
    for item in all_rows:
        if item["lat"] is None or item["lon"] is None:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [item["lon"], item["lat"]],
                },
                "properties": {
                    "district": item["district"],
                    "district_en": item["district_en"],
                    "code": item["code"],
                    "name": item["name"],
                    "name_en": item["name_en"],
                    "address": item["address"],
                    "address_en": item["address_en"],
                    "hours_weekday": item["hours_weekday"],
                    "hours_sat": item["hours_sat"],
                    "hours_sun_holiday": item["hours_sun_holiday"],
                    "hours_weekday_en": item["hours_weekday_en"],
                    "hours_sat_en": item["hours_sat_en"],
                    "hours_sun_holiday_en": item["hours_sun_holiday_en"],
                },
            }
        )

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }
    STORES_GEOJSON.write_text(json.dumps(geojson, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build SF HK store datasets for local map site.")
    parser.add_argument("--refresh-source", action="store_true", help="Re-download source HTML pages.")
    parser.add_argument("--skip-official-api", action="store_true", help="Skip calling SF official API.")
    parser.add_argument("--api-timeout", type=int, default=20, help="Timeout (seconds) for SF official API call.")
    args = parser.parse_args()

    zh_html = load_source_html(SOURCE_URL_ZH, SOURCE_HTML_ZH, refresh=args.refresh_source, required=True)
    en_html = load_source_html(SOURCE_URL_EN, SOURCE_HTML_EN, refresh=args.refresh_source, required=False)

    hk_section = extract_hk_section(zh_html)
    stores = parse_stores_zh(hk_section)
    print(f"Parsed HK store rows: {len(stores)}")

    english_map = parse_english_map(en_html)
    print(f"English rows by code: {len(english_map)}")

    official_map = fetch_official_store_map(timeout=max(args.api_timeout, 5), skip_official_api=args.skip_official_api)
    print(f"Official API rows by code: {len(official_map)}")

    rows, fallback_rows, en_fallback_count = build_rows(stores, english_map, official_map)
    print(f"Rows with fallback coordinates: {len(fallback_rows)}")
    print(f"Rows with fallback English address: {en_fallback_count}")

    write_outputs(rows, fallback_rows)
    print(f"Wrote: {STORES_JSON}")
    print(f"Wrote: {STORES_GEOJSON}")
    print(f"Wrote: {FALLBACK_JSON}")


if __name__ == "__main__":
    main()
