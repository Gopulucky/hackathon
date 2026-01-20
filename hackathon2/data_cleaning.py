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


# Excel's maximum rows per sheet
EXCEL_MAX_ROWS = 1048576


def split_and_save(df, output_base, max_rows=EXCEL_MAX_ROWS):
    """
    Split a dataframe into multiple parts if it exceeds max_rows.
    
    Parameters:
    -----------
    df : pd.DataFrame - The dataframe to split
    output_base : str - Base path for output (e.g., 'cleaned_data/biometric_cleaned')
    max_rows : int - Maximum rows per file (default: Excel limit)
    
    Returns:
    --------
    list of dicts with file info: [{'file': path, 'rows': count, 'size_mb': size}, ...]
    """
    total_rows = len(df)
    files_info = []
    
    if total_rows <= max_rows:
        # No splitting needed, but still use part1 naming for consistency
        output_file = f"{output_base}_part1.csv"
        df.to_csv(output_file, index=False)
        size_mb = os.path.getsize(output_file) / (1024 * 1024)
        files_info.append({
            'file': output_file,
            'rows': total_rows,
            'size_mb': size_mb
        })
    else:
        # Split into multiple parts
        num_parts = (total_rows // max_rows) + (1 if total_rows % max_rows else 0)
        
        for i in range(num_parts):
            start_idx = i * max_rows
            end_idx = min((i + 1) * max_rows, total_rows)
            
            part_df = df.iloc[start_idx:end_idx]
            output_file = f"{output_base}_part{i + 1}.csv"
            part_df.to_csv(output_file, index=False)
            
            size_mb = os.path.getsize(output_file) / (1024 * 1024)
            files_info.append({
                'file': output_file,
                'rows': len(part_df),
                'size_mb': size_mb
            })
            print(f"    Part {i + 1}: {len(part_df):,} rows ({size_mb:.2f} MB)")
    
    return files_info


def clean_dataset(input_dir, output_base, dataset_name):
    """
    Clean a single dataset and split if necessary.
    
    Parameters:
    -----------
    input_dir : str - Directory containing CSV files
    output_base : str - Base path for cleaned output (without extension)
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
    
    # ============================================================================
    # SECTION 2.3: LOGICAL SORTING (TIME-SERIES PREPARATION)
    # ============================================================================
    # Apply nested sorting: Date → State → District
    # Uses mergesort (stable sort) to preserve relative order of records
    sort_cols = ["date", "state", "district"]
    df_dedup.sort_values(
        by=sort_cols,
        ascending=[True, True, True],
        inplace=True,
        kind="mergesort"  # Stable sort algorithm
    )
    df_dedup.reset_index(drop=True, inplace=True)
    print(f"Applied time-series sorting (Date→State→District)")
    
    # Reorder columns (state_original at end for reference)
    cols = [c for c in df_dedup.columns if c != 'state_original'] + ['state_original']
    df_dedup = df_dedup[cols]
    
    # Split and save cleaned data
    print(f"\nSaving files (Excel limit: {EXCEL_MAX_ROWS:,} rows)...")
    files_info = split_and_save(df_dedup, output_base)
    
    print(f"\n[OK] Saved {len(files_info)} file(s)")
    print(f"  Final rows: {len(df_dedup):,}")
    print(f"  Unique states: {df_dedup['state'].nunique()}")
    
    return {
        'dataset': dataset_name,
        'original_rows': original_rows,
        'duplicates_removed': duplicates_removed,
        'invalid_states': invalid_count,
        'final_rows': len(df_dedup),
        'unique_states': df_dedup['state'].nunique(),
        'files_info': files_info,
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
        f.write("8. Split large files to comply with Excel row limit\n")
    
    print(f"\n[OK] Report saved to: {output_file}")


def generate_split_summary(stats_list, output_file):
    """Generate a summary of split files."""
    with open(output_file, 'w') as f:
        f.write("="*70 + "\n")
        f.write("CLEANED DATA - SPLIT FILES SUMMARY\n")
        f.write("="*70 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*70 + "\n\n")
        f.write("NOTE: Files have been split to comply with Excel's row limit\n")
        f.write(f"      Excel maximum rows per sheet: {EXCEL_MAX_ROWS:,}\n")
        f.write("\n" + "="*70 + "\n")
        
        for stats in stats_list:
            f.write(f"\n{stats['dataset']} DATASET\n")
            f.write("-"*70 + "\n")
            
            for file_info in stats['files_info']:
                f.write(f"  {os.path.basename(file_info['file'])}\n")
                f.write(f"    Rows: {file_info['rows']:,}\n")
                f.write(f"    Size: {file_info['size_mb']:.2f} MB\n")
            
            total_rows = sum(fi['rows'] for fi in stats['files_info'])
            f.write(f"\n  Total: {len(stats['files_info'])} file(s), {total_rows:,} rows\n")
        
        f.write("\n" + "="*70 + "\n")
        f.write("USAGE INSTRUCTIONS\n")
        f.write("="*70 + "\n")
        f.write("1. Each part file can be opened separately in Excel\n")
        f.write("2. For complete analysis, combine all parts using Python/pandas\n")
        f.write("3. All files maintain the same column structure\n")
        f.write("4. Data is split sequentially (no data loss)\n")
        f.write("\nExample Python code to combine:\n")
        f.write("  import pandas as pd\n")
        f.write("  import glob\n")
        f.write("  files = glob.glob('biometric_cleaned_part*.csv')\n")
        f.write("  df = pd.concat([pd.read_csv(f) for f in files])\n")
    
    print(f"[OK] Split summary saved to: {output_file}")


def main():
    """Main function to clean all datasets."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create output directory
    output_dir = os.path.join(base_dir, 'cleaned_data')
    os.makedirs(output_dir, exist_ok=True)
    
    # Define datasets (output_base is path without extension for splitting)
    datasets = [
        {
            'input_dir': os.path.join(base_dir, 'api_data_aadhar_biometric'),
            'output_base': os.path.join(output_dir, 'biometric_cleaned'),
            'name': 'BIOMETRIC'
        },
        {
            'input_dir': os.path.join(base_dir, 'api_data_aadhar_demographic'),
            'output_base': os.path.join(output_dir, 'demographic_cleaned'),
            'name': 'DEMOGRAPHIC'
        },
        {
            'input_dir': os.path.join(base_dir, 'api_data_aadhar_enrolment'),
            'output_base': os.path.join(output_dir, 'enrolment_cleaned'),
            'name': 'ENROLMENT'
        },
    ]
    
    # Process each dataset
    all_stats = []
    for ds in datasets:
        stats = clean_dataset(ds['input_dir'], ds['output_base'], ds['name'])
        all_stats.append(stats)
    
    # Generate reports
    report_file = os.path.join(output_dir, 'cleaning_report.txt')
    generate_report(all_stats, report_file)
    
    # Generate split files summary
    split_summary_file = os.path.join(output_dir, 'SPLIT_FILES_SUMMARY.txt')
    generate_split_summary(all_stats, split_summary_file)
    
    print("\n" + "="*60)
    print("DATA CLEANING COMPLETE!")
    print("="*60)
    print(f"\nOutput files saved to: {output_dir}")


if __name__ == '__main__':
    main()
