import pandas as pd
from itertools import product

# Set input/output paths
input_path = r"C:\Users\rexoh\Downloads\FY26_detentionStats[current].xlsx"
output_path = r"C:\Users\rexoh\Desktop\Data Hold\Cleaned\criminality_agency_pies.csv"

# Read only A91:N99 now (the valid data block)
df = pd.read_excel(
    input_path,
    sheet_name="Detention FY26",
    usecols="A:N",
    skiprows=90,
    nrows=9  # Only rows A91–A99
)

# Rename columns
df.columns = ['Label', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar',
              'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'FY Overall']

# Define mappings
criminality_mapping = {
    "Convicted Criminal": "Convicted",
    "Pending Criminal Charges": "Pending",
    "Other Immigration Violator": "Other Violation"
}
months = ['Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep']
agencies = ['CBP', 'ICE']

# Create full template using mapped values
template = pd.DataFrame(product(agencies, criminality_mapping.values(), months),
                        columns=['Agency', 'Criminality', 'Month'])

# Extract values from sheet
rows = []
agency = None

for _, row in df.iterrows():
    label = str(row['Label']).strip()
    if label.startswith("CBP Average"):
        agency = "CBP"
        continue
    elif label.startswith("ICE Average"):
        agency = "ICE"
        continue
    elif agency is None:
        continue  # Skip until agency is assigned

    if label in criminality_mapping:
        for month in months:
            val = row[month]
            try:
                if pd.notna(val) and val != "-":
                    count = int(float(str(val).replace(",", "")))
                    rows.append({
                        "Agency": agency,
                        "Criminality": criminality_mapping[label],
                        "Month": month,
                        "Count": count
                    })
            except Exception as e:
                print(f"Skipping value {val} for {agency}-{label}-{month}: {e}")

# Merge into full grid
values_df = pd.DataFrame(rows)
final_df = pd.merge(template, values_df, on=['Agency', 'Criminality', 'Month'], how='left')

# Save to file
final_df.to_csv(output_path, index=False)
print(f"Cleaned data saved to:\n{output_path}")
