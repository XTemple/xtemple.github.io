import pandas as pd

# File paths
input_path = r"C:\Users\rexoh\Downloads\FY26_detentionStats[current].xlsx"
output_path = r"C:\Users\rexoh\Desktop\Data Hold\Cleaned\facilities_ALOS.csv"

# Load the relevant sheet, skipping notes
df = pd.read_excel(
    input_path,
    sheet_name="Facilities FY26",
    skiprows=9
)

# Select and rename columns of interest
columns_to_keep = {
    'Name': 'Facility Name',
    'City': 'City',
    'State': 'State',
    'AOR': 'AOR',
    'FY26 ALOS': 'Avg Length of Stay (days)',
    'Male Crim': 'Male Crim',
    'Male Non-Crim': 'Male Non-Crim',
    'Female Crim': 'Female Crim',
    'Female Non-Crim': 'Female Non-Crim'
}

# Filter and rename
df_clean = df[list(columns_to_keep.keys())].rename(columns=columns_to_keep)

# Drop rows without key data
df_clean = df_clean.dropna(subset=[
    'Facility Name', 'City', 'State', 'Avg Length of Stay (days)'
])

# Convert numeric columns
numeric_cols = [
    'Avg Length of Stay (days)',
    'Male Crim',
    'Male Non-Crim',
    'Female Crim',
    'Female Non-Crim'
]

for col in numeric_cols:
    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').round(0).astype('Int64')

# Add total population column
df_clean['Total Detained Population'] = df_clean[
    ['Male Crim', 'Male Non-Crim', 'Female Crim', 'Female Non-Crim']
].sum(axis=1)

# Save to CSV
df_clean.to_csv(output_path, index=False)
print(f"Cleaned file saved to:\n{output_path}")
