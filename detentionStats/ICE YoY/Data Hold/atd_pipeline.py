# -*- coding: utf-8 -*-
# One-run pipeline:
# 1) Extract ATD tech subtable from each Excel file -> ATD_technology_costs_master.csv
# 2) Produce static-chart CSVs next to that master file

import re
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd

# ===============================
# 0) PASTE YOUR FILE LIST HERE
# ===============================
FILES = [
    r"C:\Users\rexoh\Desktop\ICE YoY\FY24_detentionStats07182024.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY24_detentionStats08292024.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY24_detentionStats09172024.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY24_detentionStats11222023.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY24_detentionStats12202023.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats01162025.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats02272025.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats03262025.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats04162025.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats05232025.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats06202025.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats07312025.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats08292025.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats09252025.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats11212024.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats12182024.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY23_detentionStats09292023.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY24_detentionStats01182024.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY24_detentionStats02292024.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY24_detentionStats03292024.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY24_detentionStats04252024.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY24_detentionStats05242024.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY24_detentionStats06202024.xlsx",
]

# ===============================
# 1) EXTRACTOR → master CSV
# ===============================

FILENAME_RE = re.compile(r"FY(?P<fy>\d{2})_detentionStats(?P<date>\d{8})\.xlsx$", re.IGNORECASE)

def parse_fy_and_date(path_str: str):
    m = FILENAME_RE.search(Path(path_str).name)
    if not m:
        raise ValueError(f"Unexpected filename format: {path_str}")
    fy_suffix = m.group("fy")
    date_str = m.group("date")  # mmddyyyy
    dt = datetime.strptime(date_str, "%m%d%Y").date()
    return f"FY{fy_suffix}", dt

def find_subtable(df: pd.DataFrame):
    headers_needed = {"technology", "count", "daily tech cost"}
    for i, row in df.iterrows():
        vals = [(str(x).strip().lower() if pd.notna(x) else "") for x in row.tolist()]
        if headers_needed.issubset(set(vals)):
            return i, [vals.index("technology"), vals.index("count"), vals.index("daily tech cost")]
    return None, None

def clean_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(r"[,$]", "", regex=True).str.strip(), errors="coerce")

def extract_from_file(path_str: str) -> pd.DataFrame:
    fy, dt = parse_fy_and_date(path_str)
    sheet_name = f"ATD {fy} YTD"
    df_raw = pd.read_excel(path_str, sheet_name=sheet_name, header=None, engine="openpyxl")
    start_row, cols = find_subtable(df_raw)
    if start_row is None:
        raise RuntimeError(f"Could not find subtable headers in sheet '{sheet_name}' of {path_str}")
    sub = df_raw.iloc[start_row + 1 :, cols].copy()
    sub.columns = ["Technology", "Count", "Daily Tech Cost"]
    # stop at first all-blank row
    empty_mask = sub.isna().all(axis=1)
    if empty_mask.any():
        first_empty_idx = empty_mask.idxmax()
        if empty_mask.loc[first_empty_idx]:
            sub = sub.loc[: first_empty_idx - 1]
    sub = sub.dropna(how="all")
    sub = sub[sub["Technology"].astype(str).str.strip().ne("")]
    sub["Count"] = clean_numeric(sub["Count"])
    sub["Daily Tech Cost"] = clean_numeric(sub["Daily Tech Cost"])
    sub.insert(0, "Date", pd.to_datetime(dt))
    sub.insert(1, "Fiscal Year", fy)
    sub["Source File"] = Path(path_str).name
    return sub.reset_index(drop=True)

def build_master(files) -> Path:
    parts = []
    for p in files:
        try:
            part = extract_from_file(p)
            parts.append(part)
            print(f"[OK] {p} -> {len(part)} rows")
        except Exception as e:
            print(f"[SKIP] {p} | {e}")
    if not parts:
        raise SystemExit("No data extracted.")
    master = pd.concat(parts, ignore_index=True)
    cols_order = ["Date", "Fiscal Year", "Technology", "Count", "Daily Tech Cost", "Source File"]
    master = master[cols_order]
    out_dir = Path(files[0]).parent
    out_path = out_dir / "ATD_technology_costs_master.csv"
    master.to_csv(out_path, index=False)
    print(f"\nSaved {len(master)} rows to: {out_path}")
    return out_path

# ===============================
# 2) BUILD STATIC-CHART CSVs
# ===============================

NORM_MAP = {
    "dual tech": "Dual Technology",
    "dual technology": "Dual Technology",
    "no tech": "No Technology",
    "no technology": "No Technology",
    "wristworn": "Wristworn",
    "ankle monitor": "Ankle Monitor",
    "gps": "GPS",
    "smartlink": "SmartLINK",
    "voiceid": "VoiceID",
    "veriwatch": "VeriWatch",
    "veriwatch ": "VeriWatch",
    "veri watch": "VeriWatch",
    "total": "Total",
}

def categorize(tech: str) -> str:
    t = str(tech).strip().lower()
    if t in {"ankle monitor","gps","wristworn","veriwatch"}:
        return "Physical Tracking (highest intrusiveness)"
    if t in {"dual technology"}:
        return "Layered Monitoring (high intrusiveness)"
    if t in {"smartlink","voiceid"}:
        return "Phone/Voice Reporting (moderate intrusiveness)"
    if t in {"no technology"}:
        return "Case Mgmt / No Tech (lowest intrusiveness)"
    return "Other/Uncategorized"

WEIGHTS = {
    "Case Mgmt / No Tech (lowest intrusiveness)": 0.0,
    "Phone/Voice Reporting (moderate intrusiveness)": 1.0,
    "Layered Monitoring (high intrusiveness)": 2.0,
    "Physical Tracking (highest intrusiveness)": 3.0,
    "Other/Uncategorized": 1.0,
}

def normalize_tech(s: pd.Series) -> pd.Series:
    base = s.astype(str).str.strip()
    lower = base.str.lower()
    return lower.map(NORM_MAP).fillna(base)

def load_master(master_path: Path) -> pd.DataFrame:
    df = pd.read_csv(master_path)
    for c in ["Date","Fiscal Year","Technology","Count","Daily Tech Cost"]:
        if c not in df.columns:
            raise ValueError(f"Missing column: {c}")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Count"] = pd.to_numeric(df["Count"], errors="coerce")
    df["Daily Tech Cost"] = pd.to_numeric(df["Daily Tech Cost"], errors="coerce")
    df["Tech_norm"] = normalize_tech(df["Technology"])
    df = df[df["Tech_norm"].str.lower() != "total"]
    df = df[df["Tech_norm"].str.strip().ne("")]
    df["Category"] = df["Tech_norm"].apply(categorize)
    df["Weight"] = df["Category"].map(WEIGHTS)
    df["Month"] = df["Date"].dt.to_period("M").dt.to_timestamp()
    df["Cal_Month_Num"] = df["Date"].dt.month
    return df

def make_snapshots_by_tech(df: pd.DataFrame) -> pd.DataFrame:
    snap = (df.groupby(["Fiscal Year","Month","Tech_norm"], as_index=False)
              .agg(Count=("Count","sum"),
                   Daily_Tech_Cost=("Daily Tech Cost","sum")))
    snap["CPP"] = np.where(snap["Count"]>0, snap["Daily_Tech_Cost"]/snap["Count"], np.nan)
    return snap

def make_category_timeseries(df: pd.DataFrame) -> pd.DataFrame:
    ts = (df.groupby(["Month","Category"], as_index=False)
            .agg(Count=("Count","sum")))
    totals = ts.groupby("Month", as_index=False)["Count"].sum().rename(columns={"Count":"Total"})
    ts = ts.merge(totals, on="Month", how="left")
    ts["Share"] = np.where(ts["Total"]>0, ts["Count"]/ts["Total"], np.nan)
    return ts

def make_yoy_by_calendar_month(snap: pd.DataFrame) -> pd.DataFrame:
    s = snap.copy()
    s["Cal_Month_Num"] = s["Month"].dt.month
    agg = (s.groupby(["Fiscal Year","Cal_Month_Num","Tech_norm"], as_index=False)
             .agg(Count=("Count","sum"),
                  Daily_Tech_Cost=("Daily_Tech_Cost","sum")))
    agg["CPP"] = np.where(agg["Count"]>0, agg["Daily_Tech_Cost"]/agg["Count"], np.nan)
    fy24_mn = set(agg.loc[agg["Fiscal Year"]=="FY24","Cal_Month_Num"].unique())
    fy25_mn = set(agg.loc[agg["Fiscal Year"]=="FY25","Cal_Month_Num"].unique())
    common_mn = sorted(fy24_mn.intersection(fy25_mn))
    agg = agg[agg["Cal_Month_Num"].isin(common_mn)].copy()
    fy24 = agg[agg["Fiscal Year"]=="FY24"].set_index(["Cal_Month_Num","Tech_norm"])
    fy25 = agg[agg["Fiscal Year"]=="FY25"].set_index(["Cal_Month_Num","Tech_norm"])
    joined = fy24[["Count","Daily_Tech_Cost","CPP"]].join(
        fy25[["Count","Daily_Tech_Cost","CPP"]], how="inner", lsuffix="_FY24", rsuffix="_FY25"
    ).reset_index()
    for metric in ["Count","Daily_Tech_Cost","CPP"]:
        joined[f"{metric} Δ"] = joined[f"{metric}_FY25"] - joined[f"{metric}_FY24"]
        joined[f"{metric} Δ%"] = np.where(
            joined[f"{metric}_FY24"].abs()>0,
            joined[f"{metric} Δ"]/joined[f"{metric}_FY24"],
            np.nan
        )
    joined = joined.rename(columns={"Tech_norm":"Technology","Cal_Month_Num":"Month_Num"})
    return joined.sort_values(["Month_Num","Technology"])

def make_category_share_deltas(df: pd.DataFrame) -> pd.DataFrame:
    months_by_fy = df.groupby("Fiscal Year")["Cal_Month_Num"].unique().to_dict()
    common_mn = sorted(set(months_by_fy.get("FY24",[])).intersection(set(months_by_fy.get("FY25",[]))))
    common = df[df["Cal_Month_Num"].isin(common_mn) & df["Fiscal Year"].isin(["FY24","FY25"])].copy()
    mix = (common.groupby(["Fiscal Year","Category"], as_index=False)["Count"].sum())
    tot = mix.groupby("Fiscal Year", as_index=False)["Count"].sum().rename(columns={"Count":"Total"})
    mix = mix.merge(tot, on="Fiscal Year", how="left")
    mix["Share"] = np.where(mix["Total"]>0, mix["Count"]/mix["Total"], np.nan)
    piv = mix.pivot(index="Category", columns="Fiscal Year", values="Share").fillna(0)
    piv = piv.rename(columns={"FY24":"Share_FY24","FY25":"Share_FY25"})
    piv["Delta_pp_FY25_minus_FY24"] = (piv["Share_FY25"] - piv["Share_FY24"]) * 100.0
    return piv.reset_index().sort_values("Delta_pp_FY25_minus_FY24", ascending=False)

def make_tech_avg_yoy_counts(yoy: pd.DataFrame) -> pd.DataFrame:
    return (yoy.groupby("Technology", as_index=False)
              .agg(avg_Count_Delta=("Count Δ","mean"),
                   avg_Count_Delta_pct=("Count Δ%","mean"),
                   months_compared=("Technology","count"))
              .sort_values("avg_Count_Delta", ascending=False))

def make_burden_days_monthly(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["BurdenPiece"] = df["Count"] * df["Weight"]
    burden = (df.groupby(["Fiscal Year","Month"], as_index=False)
                .agg(People=("Count","sum"),
                     Total_Cost=("Daily Tech Cost","sum"),
                     BurdenDays=("BurdenPiece","sum")))
    burden["Cal_Month_Num"] = burden["Month"].dt.month
    return burden.sort_values(["Fiscal Year","Month"])

def main():
    # STEP 1: extract master
    master_path = build_master(FILES)

    # STEP 2: build chart CSVs
    df = load_master(master_path)
    snap = make_snapshots_by_tech(df)

    out_dir = master_path.parent

    ts = make_category_timeseries(df)
    ts.to_csv(out_dir / "ATD_category_timeseries.csv", index=False)
    print(f"[OK] {out_dir/'ATD_category_timeseries.csv'}")

    yoy = make_yoy_by_calendar_month(snap)
    yoy.to_csv(out_dir / "ATD_yoy_by_calendar_month.csv", index=False)
    print(f"[OK] {out_dir/'ATD_yoy_by_calendar_month.csv'}")

    avg_yoy = make_tech_avg_yoy_counts(yoy)
    avg_yoy.to_csv(out_dir / "atd_tech_avg_yoy_counts.csv", index=False)
    print(f"[OK] {out_dir/'atd_tech_avg_yoy_counts.csv'}")

    share = make_category_share_deltas(df)
    share.to_csv(out_dir / "atd_category_share_deltas.csv", index=False)
    print(f"[OK] {out_dir/'atd_category_share_deltas.csv'}")

    burden = make_burden_days_monthly(df)
    burden.to_csv(out_dir / "atd_burden_days_monthly.csv", index=False)
    print(f"[OK] {out_dir/'atd_burden_days_monthly.csv'}")

    # (bonus) snapshots by tech
    snap.to_csv(out_dir / "ATD_tech_monthly_snapshots.csv", index=False)
    print(f"[OK] {out_dir/'ATD_tech_monthly_snapshots.csv'}")

    print("\nAll done. Files are next to your Excel inputs.")

if __name__ == "__main__":
    main()
