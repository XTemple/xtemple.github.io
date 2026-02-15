import pandas as pd
import re
from pathlib import Path

# ----------------------------
# CONFIG
# ----------------------------
FILES = [
    r"C:\Users\rexoh\Downloads\FY24_detentionStats01182024.xlsx",
    r"C:\Users\rexoh\Downloads\FY24_detentionStats02292024.xlsx",
    r"C:\Users\rexoh\Downloads\FY24_detentionStats03292024.xlsx",
    r"C:\Users\rexoh\Downloads\FY24_detentionStats04252024.xlsx",
    r"C:\Users\rexoh\Downloads\FY24_detentionStats05242024.xlsx",
    r"C:\Users\rexoh\Downloads\FY24_detentionStats06202024.xlsx",
    r"C:\Users\rexoh\Downloads\FY24_detentionStats07182024.xlsx",
    r"C:\Users\rexoh\Downloads\FY24_detentionStats08292024.xlsx",
    r"C:\Users\rexoh\Downloads\FY24_detentionStats09172024.xlsx",
    r"C:\Users\rexoh\Downloads\FY25_detentionStats01162025.xlsx",
    r"C:\Users\rexoh\Downloads\FY25_detentionStats02272025.xlsx",
    r"C:\Users\rexoh\Downloads\FY25_detentionStats03262025.xlsx",
    r"C:\Users\rexoh\Downloads\FY25_detentionStats04162025.xlsx",
    r"C:\Users\rexoh\Downloads\FY25_detentionStats05232025.xlsx",
    r"C:\Users\rexoh\Downloads\FY25_detentionStats06202025.xlsx",
    r"C:\Users\rexoh\Downloads\FY25_detentionStats07172025.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats08292025.xlsx",
    r"C:\Users\rexoh\Desktop\ICE YoY\FY25_detentionStats09252025.xlsx",
]

OUTPUT_DIR = r"C:\Users\rexoh\Desktop\Data Hold\Yearly"
CSV_OUT = Path(OUTPUT_DIR) / "monthly_arrests_yoy.csv"

# Compare these agencies
AGENCIES = {"ICE", "CBP"}

# Month orders/mappings
FY_MONTH_ORDER = ["Oct","Nov","Dec","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep"]
FY_MONTH_TO_NUM = {m: i+1 for i, m in enumerate(FY_MONTH_ORDER)}   # Oct=1..Sep=12

CAL_MONTH_ORDER = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
CAL_MONTH_TO_NUM = {m: i+1 for i, m in enumerate(CAL_MONTH_ORDER)}  # Jan=1..Dec=12

# We will keep only Jan–Sep
JAN_SEP = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep"]

# ----------------------------
# HELPERS
# ----------------------------
def infer_fy_from_filename(path_str: str) -> str | None:
    m = re.search(r'FY(\d{2,4})', Path(path_str).name, flags=re.IGNORECASE)
    if not m:
        return None
    val = m.group(1)
    return f"FY{val[-2:]}"  # normalize FY2025 -> FY25

def find_detention_sheet(xls: pd.ExcelFile) -> str:
    for s in xls.sheet_names:
        if s.strip().lower().startswith("detention fy"):
            return s
    raise ValueError(f"No 'Detention FYxx' sheet found. Sheets: {xls.sheet_names}")

def parse_file_date_from_name(fname: str) -> pd.Timestamp:
    # Expect mmddyyyy anywhere in the filename, e.g., ...07172025.xlsx
    m = re.search(r'(\d{8})', fname)
    if not m:
        return pd.NaT
    return pd.to_datetime(m.group(1), format='%m%d%Y', errors='coerce')

def extract_agency_month_block(path: str) -> pd.DataFrame:
    """
    Reads I19:V22 from 'Detention FYxx'; keeps ICE/CBP; returns long format.
    """
    xls = pd.ExcelFile(path)
    sheet = find_detention_sheet(xls)

    df = pd.read_excel(
        path,
        sheet_name=sheet,
        usecols="I:V",
        skiprows=18,
        nrows=4,
        header=None
    )
    df.columns = ['Agency', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr',
                  'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Total']

    # Clean & filter agencies
    df['Agency'] = df['Agency'].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
    df = df[df['Agency'].str.upper().isin(AGENCIES)].copy()

    # Reshape months to long
    month_cols = [m for m in FY_MONTH_ORDER if m in df.columns]
    long_df = df.melt(id_vars='Agency', value_vars=month_cols,
                      var_name='Month', value_name='Count')

    # Numeric counts
    long_df['Count'] = pd.to_numeric(long_df['Count'], errors='coerce').astype('Int64')

    # FY + source + file date
    fy = infer_fy_from_filename(path)
    if fy is None:
        m = re.search(r'FY\s*([0-9]{2,4})', sheet, flags=re.IGNORECASE)
        fy = f"FY{m.group(1)[-2:]}" if m else "FY??"

    src = Path(path).name
    long_df['FY'] = fy
    long_df['SourceFile'] = src
    long_df['FileDate'] = parse_file_date_from_name(src)

    return long_df

# ----------------------------
# MAIN
# ----------------------------
def main():
    frames, errors = [], {}

    for p in FILES:
        try:
            df = extract_agency_month_block(p)
            if not df.empty:
                frames.append(df)
            else:
                errors[p] = "Empty after filtering ICE/CBP."
        except Exception as e:
            errors[p] = str(e)

    if not frames:
        raise RuntimeError(f"No data extracted. Errors: {errors}")

    combined = pd.concat(frames, ignore_index=True)

    # Keep only Jan–Sep
    combined = combined[combined['Month'].isin(JAN_SEP)].copy()

    # Deduplicate: keep the latest file for each (FY, Agency, Month)
    combined = (
        combined
        .sort_values(['FY', 'Agency', 'Month', 'FileDate'])
        .drop_duplicates(subset=['FY', 'Agency', 'Month'], keep='last')
    )

    # Sorting helpers
    combined['MonthNumFY'] = combined['Month'].map(FY_MONTH_TO_NUM)     # fiscal order (Jan=4)
    combined['MonthNumCal'] = combined['Month'].map(CAL_MONTH_TO_NUM)   # calendar order (Jan=1..Sep=9)

    # Final sort Jan→Sep (calendar order), then within FY/Agency
    combined['Month'] = pd.Categorical(combined['Month'], categories=JAN_SEP, ordered=True)
    combined = combined.sort_values(['FY', 'Agency', 'MonthNumCal'])

    # Save CSV
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    cols = ['FY','Month','MonthNumCal','MonthNumFY','Agency','Count','SourceFile','FileDate']
    combined[cols].to_csv(CSV_OUT, index=False)

    print(f"Saved CSV: {CSV_OUT}")
    if errors:
        print("\nSome files had issues:")
        for k, v in errors.items():
            print(f" - {k}: {v}")

if __name__ == "__main__":
    main()
