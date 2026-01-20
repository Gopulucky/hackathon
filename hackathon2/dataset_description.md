# Dataset Description

## Data Sources

The analysis utilizes Aadhaar transaction data provided in multiple fragmented CSV files. These files contain records of three primary transaction types:

- **Biometric Updates**: Records of biometric information refreshes
- **Demographic Updates**: Records of demographic information modifications
- **New Enrollments**: Records of new Aadhaar registrations

## Dataset Structure

The consolidated dataset contains the following key columns:

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| Date | Date | Transaction date in DD-MM-YYYY format (standardized to ISO 8601: YYYY-MM-DD) |
| State | String | Name of the state where transaction occurred (standardized to official nomenclature) |
| District | String | Name of the district where transaction occurred |
| Pin Code | Integer | Six-digit postal code identifying specific geographic location |
| Age Group | Categorical | Classification of enrollee/updater age (0-5, 5-17, 18+) |
| Transaction Type | Categorical | Type of transaction (Biometric Update, Demographic Update, New Enrollment) |
| Count | Integer | Number of transactions for the given combination of attributes |

## Data Quality and Preprocessing

### Data Quality Issues Addressed

The raw datasets exhibited several quality issues that required preprocessing:

- **Fragmentation**: Data scattered across multiple CSV files requiring consolidation
- **Duplicate Records**: Exact duplicate entries that could skew aggregate counts
- **Inconsistent Naming**: State names with variations (e.g., "Orissa" vs. "Odisha", "Uttaranchal" vs. "Uttarakhand")
- **Date Format Variations**: Multiple date formats requiring standardization
- **Missing Values**: Incomplete records requiring validation and handling

### Data Volume

- **Total Records Analyzed**: 3,971,882 records after de-duplication
- **Time Period Covered**: 2025-03-01 to 2025-12-31
- **Geographic Coverage**: All 36 states and union territories of India
- **Total Biometric Updates**: 1,766,212
- **Total Demographic Updates**: 1,222,598
- **Total New Enrollments**: 983,072

## Data Limitations

- Dataset represents aggregated transaction counts rather than individual-level records
- Age groups are categorical rather than precise ages
- Geographic granularity limited to district and pin code level
- Temporal resolution is daily, not capturing intraday patterns
