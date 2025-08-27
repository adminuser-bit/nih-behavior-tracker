# scripts/build_ytd_2024_2025.py
# Builds small YTD (2024 vs 2025) aggregates from pages/data/reporter/nih_awards_all.csv.zst
# Output:
#   pages/data/ytd_2024_2025.json  (weekly aggregates with dims so JS can filter)
#   pages/data/picklists.json      (dropdown values + meta cutoffs)

from __future__ import annotations
import json, math, sys
from pathlib import Path
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "pages" / "data"
SRC1 = BASE / "reporter" / "nih_awards_all.csv.zst"   # primary (what you have)
SRC2 = BASE / "reporter" / "nih_awards_all.csv"       # fallback (if not compressed)

TZ = ZoneInfo("America/New_York")

def _first_existing(*paths: Path) -> Path | None:
    for p in paths:
        if p.exists():
            return p
    return None

def _pick_col(cols, priority_exact, priority_contains=()):
    cl = [c.lower() for c in cols]
    # exact (case-insensitive)
    for want in priority_exact:
        if want.lower() in cl:
            return cols[cl.index(want.lower())]
    # substring contains
    for c in cols:
        lc = c.lower()
        if any(sub in lc for sub in priority_contains):
            return c
    return None

def _normalize_org(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
         .astype(str)
         .str.strip()
         .str.replace(r"\s+", " ", regex=True)
    )

def _map_mechanism(activity: pd.Series) -> pd.Series:
    a = activity.fillna("").astype(str).str.strip()
    mech = a.str[0].str.upper()
    mech = mech.where(mech.isin(list("RUPKT")), other="Other")
    return mech

def _map_type_category(df: pd.DataFrame, type_code_col: str | None, type_text_col: str | None) -> pd.Series:
    # Return one of: new, competing_renewal, supplement, extension, noncompeting_continuation, other
    if type_code_col is not None:
        # NIH convention: 1=New, 2=Competing Renewal, 3=Supplement, 4=Extension, 5=Non-competing Continuation
        code = pd.to_numeric(df[type_code_col], errors="coerce").fillna(-1).astype(int)
        return (
            np.select(
                [
                    code.eq(1),
                    code.eq(2),
                    code.eq(3),
                    code.eq(4),
                    code.eq(5),
                ],
                ["new", "competing_renewal", "supplement", "extension", "noncompeting_continuation"],
                default="other",
            )
        )
    if type_text_col is not None:
        t = df[type_text_col].fillna("").astype(str).str.lower()
        def pick(x: str) -> str:
            if "new" in x:
                return "new"
            if "comp" in x and ("renew" in x or "continuation" in x):
                return "competing_renewal"
            if "supp" in x:
                return "supplement"
            if "non" in x and "comp" in x:
                return "noncompeting_continuation"
            if "exten" in x:
                return "extension"
            return "other"
        return t.map(pick)
    return pd.Series(["other"] * len(df), index=df.index)

def _same_month_day(year_ref: int, month: int, day: int) -> date:
    # If asking for Feb 29 on a non-leap year, return Feb 28
    try:
        return date(year_ref, month, day)
    except ValueError:
        # Likely Feb 29 -> use Feb 28
        return date(year_ref, month, 28)

def main() -> int:
    src = _first_existing(SRC1, SRC2)
    if not src:
        print(f"ERROR: Could not find {SRC1} or {SRC2}", file=sys.stderr)
        return 2

    # Load all columns; file is only a few MB compressed
    compression = "zstd" if src.suffix == ".zst" else "infer"
    df = pd.read_csv(src, compression=compression, low_memory=False)

    cols = list(df.columns)

    # Detect columns (robust to naming differences)
    date_col = _pick_col(
        cols,
        priority_exact=[
            "award_notice_date", "notice_date", "action_date", "award_date", "date"
        ],
        priority_contains=["date"],
    )
    if not date_col:
        raise RuntimeError(f"Could not detect a date column. Columns: {cols[:25]}...")

    # Try many common names first…
amount_col = _pick_col(
    cols,
    priority_exact=[
        # per-action / obligation style
        "award_amount","award_amount_usd","amount_this_action","action_amount",
        "obligation_amount","obligated_amount","award_obligated_amount",
        "award_amount_current_usd","current_amount","transaction_amount",
        # total-style fallbacks (calendar/FY totals on the row)
        "fy_total_cost","total_cost","total_cost_subproject",
        "total_cost_amount","project_total_cost","award_total_cost",
        "direct_cost","direct_cost_amt","direct_costs"
    ],
    priority_contains=["amount","oblig","cost","dollar"],
)

# If still not found, heuristically pick the first *numeric* column whose name smells like dollars.
if not amount_col:
    import pandas as pd
    num_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    cand = []
    for c in num_cols:
        lc = c.lower()
        if any(s in lc for s in ("amount","oblig","cost","dollar","total")):
            cand.append(c)
    if cand:
        amount_col = cand[0]
        print(f"[info] Using heuristic amount column: {amount_col}")

if not amount_col:
    # Last-ditch: print everything so the logs show us what to target next time
    print("DEBUG all columns:", list(df.columns), file=sys.stderr)
    raise RuntimeError("Could not detect an amount column.")


    org_col = _pick_col(cols, ["org_name", "organization", "organization_name", "org_name_norm"], ["org"])
    if not org_col:
        raise RuntimeError(f"Could not detect an organization column. Columns: {cols[:25]}...")

    ic_col = _pick_col(cols, ["admin_ic", "ic", "institute", "administrative_ic"], ["ic"])
    if not ic_col:
        raise RuntimeError(f"Could not detect an IC column. Columns: {cols[:25]}...")

    act_col = _pick_col(cols, ["activity_code", "activity"], ["activity"])
    # activity code is helpful but optional

    type_code_col = _pick_col(cols, ["type_code", "award_type_code"], [])
    type_text_col = _pick_col(cols, ["award_type", "type"], ["type"])

    # Parse and clean
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True).dt.tz_convert(TZ).dt.date
    df = df[df[date_col].notna()]

    # Amount numeric
    df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0)

    # Basic fields
    df["org_name_norm"] = _normalize_org(df[org_col])
    df["admin_ic"] = df[ic_col].fillna("").astype(str).str.strip().str.upper()
    if act_col:
        df["activity_code"] = df[act_col].fillna("").astype(str).str.strip().str.upper()
        df["mechanism"] = _map_mechanism(df["activity_code"])
    else:
        df["activity_code"] = ""
        df["mechanism"] = "Other"

    df["type_category"] = _map_type_category(df, type_code_col, type_text_col)

    # YTD windows (calendar, to "today" in ET)
    today = datetime.now(TZ).date()
    end_2025 = min(today, date(2025, 12, 31))
    end_2024 = _same_month_day(2024, end_2025.month, end_2025.day)

    mask_2025 = (df[date_col] >= date(2025, 1, 1)) & (df[date_col] <= end_2025)
    mask_2024 = (df[date_col] >= date(2024, 1, 1)) & (df[date_col] <= end_2024)
    df = df[mask_2025 | mask_2024].copy()

    # Keep only the types we will expose via filters to keep file small
    keep_types = {"new", "competing_renewal", "supplement"}
    df = df[df["type_category"].isin(keep_types)]

    # Week aggregation to keep size small
    # week_start = Monday of that week (calendar weeks)
    dt = pd.to_datetime(df[date_col])
    week_start = (dt - pd.to_timedelta(dt.dt.weekday, unit="D")).dt.date
    df["year"] = pd.to_datetime(df[date_col]).dt.year
    df["week_start"] = week_start
    df["week_of_year"] = pd.to_datetime(df["week_start"]).dt.isocalendar().week.astype(int)

    # Aggregate
    group_cols = [
        "year", "week_of_year", "week_start",
        "admin_ic", "mechanism", "activity_code", "org_name_norm", "type_category"
    ]
    agg = (
        df.groupby(group_cols, dropna=False, as_index=False)[amount_col]
          .sum()
          .rename(columns={amount_col: "amount"})
    )

    BASE.mkdir(parents=True, exist_ok=True)
    out = BASE / "ytd_2024_2025.json"
    # Safe JSON (numbers), no NaN
    records = agg.fillna({"admin_ic":"", "mechanism":"", "activity_code":"", "org_name_norm":"", "type_category":"other"}).to_dict(orient="records")
    with open(out, "w") as f:
        json.dump(records, f)

    # Picklists
    pick = {
        "institutions": sorted([x for x in agg["org_name_norm"].dropna().unique().tolist() if x]),
        "ics":          sorted([x for x in agg["admin_ic"].dropna().unique().tolist() if x]),
        "mechanisms":   sorted([x for x in agg["mechanism"].dropna().unique().tolist() if x]),
        "activity_codes": sorted([x for x in agg["activity_code"].dropna().unique().tolist() if x]),
        "type_options": ["new", "competing_renewal", "supplement"],
        "meta": {
            "cutoff_2025": end_2025.isoformat(),
            "cutoff_2024": end_2024.isoformat(),
            "source_file": str(src),
        },
    }
    with open(BASE / "picklists.json", "w") as f:
        json.dump(pick, f, indent=2)

    print(f"Wrote {out}")
    print(f"Wrote {BASE/'picklists.json'}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
