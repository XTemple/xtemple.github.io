import pandas as pd

# Load Excel sheet
df = pd.read_excel(
    r"C:\Users\rexoh\Downloads\FY26_detentionStats[current].xlsx",
    sheet_name="Facilities FY26",
    skiprows=9
)

# Columns of interest
columns_to_keep = [
    'State',
    'Male Crim',
    'Male Non-Crim',
    'Female Crim',
    'Female Non-Crim'
]

# Drop rows missing critical data
df = df[columns_to_keep].dropna(subset=['State'])

# Convert to numeric and round to whole numbers
for col in columns_to_keep[1:]:
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(0).astype(int)

# Add total columns
df['Total Crim'] = df['Male Crim'] + df['Female Crim']
df['Total Non-Crim'] = df['Male Non-Crim'] + df['Female Non-Crim']
df['Total Population (Crim + Non-Crim)'] = df['Total Crim'] + df['Total Non-Crim']

# Save cleaned data
df.to_csv(r"C:\Users\rexoh\Desktop\Data Hold\Cleaned\criminality_by_sex.csv", index=False)
print("Cleaned file exported.")
