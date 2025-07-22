import pandas as pd

# File paths
input_path = r"C:\Users\rexoh\Downloads\FY25_detentionStats[current].xlsx"
output_path = r"C:\Users\rexoh\Desktop\Workspaces\detentionStats\Cleaned\facilities_full_current.csv"

# Load the relevant sheet, skipping notes
df = pd.read_excel(
    input_path,
    sheet_name="Facilities FY25",
    skiprows=6
)

# Select and rename columns of interest
columns_to_keep = {
    'Name': 'Facility Name',
    'City': 'City',
    'State': 'State',
    'AOR': 'AOR',
    'FY25 ALOS': 'Avg Length of Stay (days)',
    'No ICE Threat Level': 'No ICE Threat Level Population',
    'Male Crim': 'Male Crim',
    'Male Non-Crim': 'Male Non-Crim',
    'Female Crim': 'Female Crim',
    'Female Non-Crim': 'Female Non-Crim'
}

# Filter and rename
df_clean = df[list(columns_to_keep.keys())].rename(columns=columns_to_keep)

# Drop rows without key data
df_clean = df_clean.dropna(subset=[
    'Facility Name', 'City', 'State',
    'Avg Length of Stay (days)', 'No ICE Threat Level Population'
])

# Round numeric columns
for col in [
    'Avg Length of Stay (days)', 'No ICE Threat Level Population',
    'Male Crim', 'Male Non-Crim', 'Female Crim', 'Female Non-Crim'
]:
    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').round(0).astype('Int64')

# Save to fixed filename
df_clean.to_csv(output_path, index=False)
print(f"Full facility data saved to:\n{output_path}")
