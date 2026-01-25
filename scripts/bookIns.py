import pandas as pd

# Path to your Excel file
xlsx_path = r"C:\Users\rexoh\Downloads\FY26_detentionStats[current].xlsx"

# Read I19:V22 and assign column names manually
monthly_raw = pd.read_excel(
    xlsx_path,
    sheet_name="Detention FY26",
    usecols="I:V",
    skiprows=18,
    nrows=4
)
monthly_raw.columns = ['Agency', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr',
                       'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Total']

# Keep only ICE and CBP rows
monthly_filtered = monthly_raw.iloc[1:3]

# Reshape to long format
monthly_long = monthly_filtered.melt(
    id_vars='Agency',
    var_name='Month',
    value_name='Count'
)

# Clean numbers
monthly_long['Count'] = pd.to_numeric(monthly_long['Count'], errors='coerce').astype('Int64')

# Output file path (fixed name for Power BI compatibility)
output_path = r"C:\Users\rexoh\Desktop\Data Hold\Cleaned\monthly_arrest_lines.csv"

# Save to CSV
monthly_long.to_csv(output_path, index=False)

print(f"Book-in data saved to: {output_path}")
