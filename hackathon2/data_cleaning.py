"""
Professional Data Cleaning Script for Aadhaar Datasets
======================================================
Cleans three datasets: Biometric, Demographic, Enrolment
- State name standardization (66 → 36 official names)
- Duplicate removal
- Date format standardization
- Data validation
"""

import pandas as pd
import glob
import os
from datetime import datetime

# ============================================================================
# STATE NAME STANDARDIZATION MAPPING
# ============================================================================
STATE_MAPPING = {
    # Andaman & Nicobar Islands
    'andaman & nicobar islands': 'Andaman and Nicobar Islands',
    'andaman and nicobar islands': 'Andaman and Nicobar Islands',
    
    # Andhra Pradesh
    'andhra pradesh': 'Andhra Pradesh',
    
    # Arunachal Pradesh
    'arunachal pradesh': 'Arunachal Pradesh',
    
    # Assam
    'assam': 'Assam',
    
    # Bihar
    'bihar': 'Bihar',
    
    # Chandigarh
    'chandigarh': 'Chandigarh',
    
    # Chhattisgarh (handle old spelling)
    'chhattisgarh': 'Chhattisgarh',
    'chhatisgarh': 'Chhattisgarh',
    
    # Dadra and Nagar Haveli and Daman and Diu (merged UT)
    'dadra & nagar haveli': 'Dadra and Nagar Haveli and Daman and Diu',
    'dadra and nagar haveli': 'Dadra and Nagar Haveli and Daman and Diu',
    'dadra and nagar haveli and daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'the dadra and nagar haveli and daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'daman & diu': 'Dadra and Nagar Haveli and Daman and Diu',
    'daman and diu': 'Dadra and Nagar Haveli and Daman and Diu',
    
    # Delhi
    'delhi': 'Delhi',
    
    # Goa
    'goa': 'Goa',
    
    # Gujarat
    'gujarat': 'Gujarat',
    
    # Haryana
    'haryana': 'Haryana',
    
    # Himachal Pradesh
    'himachal pradesh': 'Himachal Pradesh',
    
    # Jammu and Kashmir
    'jammu & kashmir': 'Jammu and Kashmir',
    'jammu and kashmir': 'Jammu and Kashmir',
    
    # Jharkhand
    'jharkhand': 'Jharkhand',
    
    # Karnataka
    'karnataka': 'Karnataka',
    
    # Kerala
    'kerala': 'Kerala',
    
    # Ladakh
    'ladakh': 'Ladakh',
    
    # Lakshadweep
    'lakshadweep': 'Lakshadweep',
    
    # Madhya Pradesh
    'madhya pradesh': 'Madhya Pradesh',
    
    # Maharashtra
    'maharashtra': 'Maharashtra',
    
    # Manipur
    'manipur': 'Manipur',
    
    # Meghalaya
    'meghalaya': 'Meghalaya',
    
    # Mizoram
    'mizoram': 'Mizoram',
    
    # Nagaland
    'nagaland': 'Nagaland',
    
    # Odisha (handle old name)
    'odisha': 'Odisha',
    'orissa': 'Odisha',
    
    # Puducherry (handle old name)
    'puducherry': 'Puducherry',
    'pondicherry': 'Puducherry',
    
    # Punjab
    'punjab': 'Punjab',
    
    # Rajasthan
    'rajasthan': 'Rajasthan',
    
    # Sikkim
    'sikkim': 'Sikkim',
    
    # Tamil Nadu
    'tamil nadu': 'Tamil Nadu',
    'tamilnadu': 'Tamil Nadu',
    
    # Telangana
    'telangana': 'Telangana',
    
    # Tripura
    'tripura': 'Tripura',
    
    # Uttar Pradesh
    'uttar pradesh': 'Uttar Pradesh',
    
    # Uttarakhand (handle old name)
    'uttarakhand': 'Uttarakhand',
    'uttaranchal': 'Uttarakhand',
    
    # West Bengal (handle all variations)
    'west bengal': 'West Bengal',
    'west  bengal': 'West Bengal',  # double space
    'west bangal': 'West Bengal',
    'west bengli': 'West Bengal',
    'westbengal': 'West Bengal',
}

# Invalid entries (city names, numbers, etc.)
INVALID_STATES = {
    '100000',
    'balanagar',
    'nagpur', 
    'jaipur',
    'madanapalle',
    'raja annamalai puram',
}


def standardize_state(state_value):
    """Standardize state name to official name."""
    if pd.isna(state_value):
        return 'INVALID'
    
    state_lower = str(state_value).strip().lower()
    
    # Check if it's an invalid entry
    if state_lower in INVALID_STATES:
        return 'INVALID'
    
    # Look up in mapping
    if state_lower in STATE_MAPPING:
        return STATE_MAPPING[state_lower]
    
    # If not found, mark as invalid
    return 'INVALID'


def clean_dataset(input_dir, output_file, dataset_name):
    """
    Clean a single dataset.
    
    Parameters:
    -----------
    input_dir : str - Directory containing CSV files
    output_file : str - Path for cleaned output CSV
    dataset_name : str - Name for logging
    
    Returns:
    --------
    dict with cleaning statistics
    """
    print(f"\n{'='*60}")
    print(f"Processing: {dataset_name}")
    print('='*60)
    
    # Load all CSV files
    csv_files = glob.glob(os.path.join(input_dir, '*.csv'))
    print(f"Found {len(csv_files)} CSV files")
    
    dfs = []
    for f in csv_files:
        df = pd.read_csv(f)
        dfs.append(df)
        print(f"  - {os.path.basename(f)}: {len(df):,} rows")
    
    df = pd.concat(dfs, ignore_index=True)
    original_rows = len(df)
    print(f"\nTotal rows loaded: {original_rows:,}")
    
    # Remove duplicates
    df_dedup = df.drop_duplicates()
    duplicates_removed = original_rows - len(df_dedup)
    print(f"Duplicates removed: {duplicates_removed:,}")
    
    # Standardize state names
    df_dedup['state_original'] = df_dedup['state'].copy()
    df_dedup['state'] = df_dedup['state'].apply(standardize_state)
    
    invalid_count = (df_dedup['state'] == 'INVALID').sum()
    print(f"Invalid state entries: {invalid_count:,}")
    
    # Standardize district names (title case, strip whitespace)
    df_dedup['district'] = df_dedup['district'].str.strip().str.title()
    
    # Convert date to standard format (YYYY-MM-DD)
    df_dedup['date'] = pd.to_datetime(df_dedup['date'], format='%d-%m-%Y', errors='coerce')
    df_dedup['date'] = df_dedup['date'].dt.strftime('%Y-%m-%d')
    
    # Validate pincode (should be 6 digits)
    df_dedup['pincode'] = df_dedup['pincode'].astype(str).str.zfill(6)
    
    # Reorder columns (state_original at end for reference)
    cols = [c for c in df_dedup.columns if c != 'state_original'] + ['state_original']
    df_dedup = df_dedup[cols]
    
    # Save cleaned data
    df_dedup.to_csv(output_file, index=False)
    print(f"\n[OK] Saved to: {output_file}")
    print(f"  Final rows: {len(df_dedup):,}")
    print(f"  Unique states: {df_dedup['state'].nunique()}")
    
    return {
        'dataset': dataset_name,
        'original_rows': original_rows,
        'duplicates_removed': duplicates_removed,
        'invalid_states': invalid_count,
        'final_rows': len(df_dedup),
        'unique_states': df_dedup['state'].nunique(),
    }


def generate_report(stats_list, output_file):
    """Generate a cleaning summary report."""
    with open(output_file, 'w') as f:
        f.write("="*70 + "\n")
        f.write("AADHAAR DATA CLEANING REPORT\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*70 + "\n\n")
        
        for stats in stats_list:
            f.write(f"\n{stats['dataset']}\n")
            f.write("-"*40 + "\n")
            f.write(f"  Original rows:      {stats['original_rows']:>12,}\n")
            f.write(f"  Duplicates removed: {stats['duplicates_removed']:>12,}\n")
            f.write(f"  Invalid states:     {stats['invalid_states']:>12,}\n")
            f.write(f"  Final rows:         {stats['final_rows']:>12,}\n")
            f.write(f"  Unique states:      {stats['unique_states']:>12}\n")
        
        f.write("\n" + "="*70 + "\n")
        f.write("CLEANING OPERATIONS PERFORMED:\n")
        f.write("="*70 + "\n")
        f.write("1. Removed exact duplicate rows\n")
        f.write("2. Standardized state names (66 variations -> official names)\n")
        f.write("3. Marked invalid state entries (city names, numbers) as 'INVALID'\n")
        f.write("4. Standardized district names (Title Case)\n")
        f.write("5. Converted dates to YYYY-MM-DD format\n")
        f.write("6. Padded pincodes to 6 digits\n")
        f.write("7. Added 'state_original' column for reference\n")
    
    print(f"\n[OK] Report saved to: {output_file}")


def main():
    """Main function to clean all datasets."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create output directory
    output_dir = os.path.join(base_dir, 'cleaned_data')
    os.makedirs(output_dir, exist_ok=True)
    
    # Define datasets
    datasets = [
        {
            'input_dir': os.path.join(base_dir, 'api_data_aadhar_biometric'),
            'output_file': os.path.join(output_dir, 'biometric_cleaned.csv'),
            'name': 'BIOMETRIC'
        },
        {
            'input_dir': os.path.join(base_dir, 'api_data_aadhar_demographic'),
            'output_file': os.path.join(output_dir, 'demographic_cleaned.csv'),
            'name': 'DEMOGRAPHIC'
        },
        {
            'input_dir': os.path.join(base_dir, 'api_data_aadhar_enrolment'),
            'output_file': os.path.join(output_dir, 'enrolment_cleaned.csv'),
            'name': 'ENROLMENT'
        },
    ]
    
    # Process each dataset
    all_stats = []
    for ds in datasets:
        stats = clean_dataset(ds['input_dir'], ds['output_file'], ds['name'])
        all_stats.append(stats)
    
    # Generate report
    report_file = os.path.join(output_dir, 'cleaning_report.txt')
    generate_report(all_stats, report_file)
    
    print("\n" + "="*60)
    print("DATA CLEANING COMPLETE!")
    print("="*60)
    print(f"\nOutput files saved to: {output_dir}")


if __name__ == '__main__':
    main()
