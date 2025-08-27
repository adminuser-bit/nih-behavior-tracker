# scripts/build_ytd_2024_2025.py
from __future__ import annotations
import json, sys, glob
from pathlib import Path
from datetime import date, datetime
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "pages" / "data"
SRC_MAIN_1 = BASE / "reporter" / "nih_awards_all.csv.zst"
SRC_MAIN_2 = BASE / "reporter" / "nih_awards_all.csv"
TZ = ZoneInfo("America/New_York")

def _first_existing(*paths: Path) -> Path | None:
    for p in paths:
        if p.exists():
            return p
    return None

def _pick_col(cols, priority_exact, priority_contains=()):
    cl = [c.lower() for c in cols]
    for want in priority_exact:
        if want.lower() in cl:
            return cols[cl.index(want.lower())]
    for c in cols:
        lc = c.lower()
        if any(sub in lc for sub in priority_contains):
            return c
    return None

def _normalize_org(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip().str.replace(r"\s+", " ", regex=True)

def _map_mechanism(activity: pd.Series) -> pd.Series:
    a = activity.fillna("").astype(str).str.strip()
    mech = a.str[0].str.upper()
    return mech.where(mech.isin(list("RUPKT")), other="Other")

def _map_type_category(df: pd.DataFrame, type_code_col: str | None, type_text_col: str | None) -> pd.Series:
    if type_code_col is not None:
        code = pd.to_numeric(df[type_code_col], errors="coerce").fillna(-1).astype(int)
        return np.select(
            [code.eq(1), code.eq(2), code.eq(3), code.eq(4), code.eq(5)],
            ["new", "competing_renewal", "supplement", "extension", "noncompeting_continuation"],
            default="other",
        )
    if type_text_col is not None:
        t = df[type_text_col].fillna("").astype(str).str.lower()
        def pick(x: str) -> str:
            if "new" in x: return "new"
            if "supp" in x: return "supplement"
            if "non" in x and "comp" in x: return "noncompeting_continuation"
            if "exten" in x: return "extension"
            if "comp" in x and ("renew" in x or "continuation" in x): return "competing_renewal"
            return "other"
        return t.map(pick)
    return pd.Series(["other"] * len(df), index=df.index)

def _same_month_day(year_ref: int, month: int, day: int) -> date:
    try:
        return date(year_ref, month, day)
    except ValueError:
        return date(year_ref, month, 28)

def _load_csv_any(path: Path) -> pd.DataFrame:
    comp = "zstd" if path.suffix == ".zst" else "infer"
    return pd.read_csv(path, compression=comp, low_memory=False)

def _detect_amount_col(cols) -> str | None:
    return _pick_col(
        cols,
        [
            "award_amount","award_amount_usd","amount_this_action","action_amount",
            "obligation_amount","obligated_amount","award_obligated_amount",
            "award_amount_current_usd","current_amount","transaction_amount",
            "fy_total_cost","total_cost","total_cost_subproject",
            "total_cost_amount","project_total_cost","award_total_cost",
            "direct_cost","direct_cost_amt","direct_costs",
        ],
        ["amount","oblig","cost","dollar","total"],
    )

def _detect_date_col(cols) -> str | None:
    return _pick_col(
        cols,
        ["award_notice_date","notice_date","action_date","award_date","date","notice_dt","award_dt","transaction_date"],
        ["notice","award","action","date"],
    )

def _merge_in_amounts_if_needed(df: pd.DataFrame, date_col: str, grant_col: str) -> tuple[pd.DataFrame, str]:
    """
    If df has no amount column, try to merge one from pages/data/reporter/*amount*
    Matching keys: (grant_number, award_date) with tolerant column name detection in the sidecar.
    Returns (df_with_amount, amount_col_name).
    """
    amount_col = _detect_amount_col(df.columns)
    if amount_col:
        return df, amount_col

    # Look for sidecar files we copied (CSV/CSV.ZST/Feather/Parquet CSVs preferred)
    candidates = []
    for pat in ("*.csv.zst", "*.csv", "*.feather", "*.parquet"):
        candidates.extend(glob.glob(str((BASE/"reporter")/f"*amount*{pat}")))
    if not candidates:
        raise RuntimeError(
            "No dollar column found in main file AND no sidecar amount file under pages/data/reporter/*amount*.\n"
            "Fix: ensure the upstream processed award-amounts CSV is copied (e.g., nih_award_amounts*.csv.zst)."
        )

    for path in candidates:
        p = Path(path)
        try:
            cand = _load_csv_any(p) if p.suffix in (".csv", ".zst") else pd.read_feather(p) if p.suffix == ".feather" else pd.read_parquet(p)
        except Exception as e:
            print(f"[warn] Could not read {p}: {e}", file=sys.stderr)
            continue

        cols = list(cand.columns)
        amt2 = _detect_amount_col(cols)
        if not amt2:
            continue
        date2 = _detect_date_col(cols)
        grant2 = _pick_col(cols, ["grant_number","core_project_num","project_num","project_number","application_id"], ["grant","project","application"])
        if not grant2:
            continue

        # Parse dates for join
        cand = cand.copy()
        if date2:
            cand[date2] = pd.to_datetime(cand[date2], errors="coerce", utc=True).dt.tz_convert(TZ).dt.date
        cand = cand[[c for c in {grant2, date2, amt2} if c]].dropna(subset=[amt2])

        # Attempt strict join first (grant + date). If sidecar lacks date, fall back to grant only.
        df_join = df.copy()
        if date2:
            m1 = df_join.merge(
                cand,
                left_on=[grant_col, date_col],
                right_on=[grant2, date2],
                how="left",
                suffixes=("", "_amt"),
            )
        else:
            m1 = df_join.merge(
                cand[[grant2, amt2]].groupby(grant2, as_index=False)[amt2].sum(),
                left_on=grant_col, right_on=grant2, how="left", suffixes=("", "_amt")
            )

        if m1[amt2].notna().any():
            print(f"[info] merged award amounts from {p.name} using keys: {grant_col} + {('date' if date2 else 'grant only')}")
            return m1.rename(columns={amt2: "award_amount_merged"}), "award_amount_merged"

    # If we get here, we tried all sidecars and failed
    raise RuntimeError(
        "Could not obtain dollars. The main file has no amount column and none of the sidecar '*amount*' files "
        "could be merged on (grant_number[, award_date])."
    )

def main() -> int:
    src = _first_existing(SRC_MAIN_1, SRC_MAIN_2)
    if not src:
        print(f"ERROR: Could not find {SRC_MAIN_1} or {SRC_MAIN_2}", file=sys.stderr)
        return 2

    df = _load_csv_any(src)
    cols = list(df.columns)
    print("[info] main columns:", cols[:20])

    # Detect essential columns
    date_col = _detect_date_col(cols)
    if not date_col:
        raise RuntimeError("Could not detect a date column in main file.")
    grant_col = _pick_col(cols, ["grant_number","core_project_num","project_num","project_number","application_id"], ["grant","project","application"])
    if not grant_col:
        raise RuntimeError("Could not detect a grant/project identifier column in main file.")
    ic_col = _pick_col(cols, ["admin_ic","ic","ic_code","institute","administrative_ic","admin_ic_name"], ["admin","ic","instit"])
    if not ic_col:
        raise RuntimeError("Could not detect an IC column in main file.")
    act_col = _pick_col(cols, ["activity_code","activity"], ["activity","code"])
    type_code_col = _pick_col(cols, ["type_code","award_type_code"], [])
    type_text_col = _pick_col(cols, ["award_type","type"], ["type"])

    # Parse basics
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True).dt.tz_convert(TZ).dt.date
    df = df[df[date_col].notna()]
    df["org_name_norm"] = _normalize_org(df.get("org_name", ""))
    df["admin_ic"] = df[ic_col].fillna("").astype(str).str.strip().str.upper()
    if act_col:
        df["activity_code"] = df[act_col].fillna("").astype(str).str.strip().str.upper()
        df["mechanism"] = _map_mechanism(df["activity_code"])
    else:
        df["activity_code"] = ""
        df["mechanism"] = "Other"
    df["type_category"] = _map_type_category(df, type_code_col, type_text_col)

    # Get a dollars column (from main or by merge)
    df, amount_col = _merge_in_amounts_if_needed(df, date_col=date_col, grant_col=grant_col)
    df["amount_value"] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0.0)

    # Calendar-YTD windows
    today = datetime.now(TZ).date()
    end_2025 = min(today, date(2025, 12, 31))
    end_2024 = _same_month_day(2024, end_2025.month, end_2025.day)
    mask_2025 = (df[date_col] >= date(2025, 1, 1)) & (df[date_col] <= end_2025)
    mask_2024 = (df[date_col] >= date(2024, 1, 1)) & (df[date_col] <= end_2024)
    df = df[mask_2025 | mask_2024].copy()

    # Keep only types we expose
    keep_types = {"new","competing_renewal","supplement"}
    df = df[df["type_category"].isin(keep_types)]

    # Weekly aggregation
    dt = pd.to_datetime(df[date_col])
    week_start = (dt - pd.to_timedelta(dt.dt.weekday, unit="D")).dt.date
    df["year"] = pd.to_datetime(df[date_col]).dt.year
    df["week_start"] = week_start
    df["week_of_year"] = pd.to_datetime(df["week_start"]).dt.isocalendar().week.astype(int)

    group_cols = ["year","week_of_year","week_start","admin_ic","mechanism","activity_code","org_name_norm","type_category"]
    agg = (
        df.groupby(group_cols, dropna=False, as_index=False)["amount_value"]
          .sum()
          .rename(columns={"amount_value":"amount"})
    )

    # Outputs
    BASE.mkdir(parents=True, exist_ok=True)
    out = BASE / "ytd_2024_2025.json"
    records = agg.fillna({"admin_ic":"","mechanism":"","activity_code":"","org_name_norm":"","type_category":"other"}).to_dict(orient="records")
    with open(out, "w") as f:
        json.dump(records, f)

    pick = {
        "institutions": sorted([x for x in agg["org_name_norm"].dropna().unique().tolist() if x]),
        "ics": sorted([x for x in agg["admin_ic"].dropna().unique().tolist() if x]),
        "mechanisms": sorted([x for x in agg["mechanism"].dropna().unique().tolist() if x]),
        "activity_codes": sorted([x for x in agg["activity_code"].dropna().unique().tolist() if x]),
        "type_options": ["new","competing_renewal","supplement"],
        "meta": {
            "cutoff_2025": end_2025.isoformat(),
            "cutoff_2024": end_2024.isoformat(),
            "source_file": str(src),
            "measurement": "amount_usd"
        },
    }
    with open(BASE / "picklists.json", "w") as f:
        json.dump(pick, f, indent=2)

    print(f"Wrote {out}")
    print(f"Wrote {BASE/'picklists.json'}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
