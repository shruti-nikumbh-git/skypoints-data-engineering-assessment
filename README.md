# skypoints-data-engineering-assessment
Azure Medallion Architecture implementation using PySpark, Delta Lake.

# SkyPoints Data Engineering Assessment

## 1. Project Overview

This project implements an end-to-end data engineering pipeline for a global airline loyalty program called **SkyPoints**.

The objective is to ingest member data from multiple country-specific Excel files, perform data cleansing and validation, transform the data using PySpark, process nested JSON redemption transactions, and generate structured datasets using the Medallion Architecture.

The pipeline follows three layers:

- **Bronze:** Raw data ingestion.
- **Silver:** Data cleansing, standardization, transformation, and validation.
- **Gold:** Business-ready country-specific member datasets and redemption transaction data.

Invalid records are separated into a quarantine layer for further investigation.

## 2. Technologies Used

| Technology | Purpose |
|---|---|
| Python | Data processing and scripting |
| PySpark | Distributed data processing |
| Delta Lake | Storage in Delta format |
| Pandas | Reading Excel source files |
| Google Colab | Development and execution environment |
| PyTest | Automated testing |
| Azure Data Lake Storage Gen2 | Proposed cloud storage |
| Azure Databricks | Proposed cloud processing platform |
| Azure Data Factory | Proposed pipeline orchestration |

## 3. Project Architecture

The solution follows the Medallion Architecture.

```text
                  SOURCE FILES
             IND.xlsx | USA.xlsx | AUS.xlsx
                         |
                         v
                  BRONZE LAYER
              Raw Data Ingestion
              Delta Lake Storage
                         |
                         v
                  SILVER LAYER
             Schema Standardization
              Date Transformations
               Age Calculation
             Stale Member Calculation
               Data Validation
                         |
                +--------+--------+
                |                 |
                v                 v
          VALID RECORDS       QUARANTINE
                |             INVALID RECORDS
                v
                  GOLD LAYER
             Country-Specific Tables
             India | USA | Australia
                         |
                         |
               JSON REDEMPTION FEED
                         |
                         v
                 JSON FLATTENING
                   PySpark Explode
                         |
                         v
                 MEMBER PROFILE JOIN
                         |
                         v
                GOLD REDEMPTION TABLE
```

## 4. Source Data

Three country-specific Excel files were used as input:

- IND.xlsx
- USA.xlsx
- AUS.xlsx

The source files contained member information such as:

- Member ID
- Member Name
- Date of Birth
- Tier Code
- Enrollment Date
- Flight Date
- Member Type

The source files had different column names and schemas. Therefore, schema standardization was performed before combining the data.

A separate nested JSON redemption feed was also used for transaction processing.

## 5. Step-by-Step Implementation

### Step 1: Environment Setup

The project was developed using Google Colab.

Installed the required libraries:

```python
!pip install -q pyspark==3.5.3 delta-spark==3.2.1
!pip install -q pandas openpyxl pytest
```

Configured Spark with Delta Lake support:

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("SkyPoints-Assessment")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("Spark Version:", spark.version)
```

### Step 2: Source File Inspection

Used Pandas to read the Excel files and inspect:

- File availability
- Sheet names
- Column names
- Number of records
- Missing values
- Source data formats

This helped identify differences in the country-specific source files.

### Step 3: Bronze Layer – Raw Data Ingestion

Created a Bronze layer using PySpark and Delta Lake.

Actions performed:

1. Read the three Excel files.
2. Standardized source column names.
3. Converted source fields into a common schema.
4. Preserved raw record information.
5. Added source country and source filename.
6. Added source sheet and ingestion timestamp.
7. Stored the combined raw data as a Delta table.

Bronze table location:

```text
/content/skypoints/data/bronze/member_raw
```

Bronze schema included:

- member_id
- member_name
- dob_raw
- tier_code
- enrollment_date_raw
- flight_date_raw
- member_type
- raw_record_json
- source_country
- source_file
- source_sheet
- ingestion_timestamp

**Bronze record count: 9**

### Step 4: Silver Layer – Data Transformation

Read the Bronze Delta data and performed data cleansing and transformations.

Actions performed:

1. Trimmed unnecessary spaces from source fields.
2. Standardized member identifiers and column formats.
3. Converted raw date fields into proper date formats.
4. Handled different date formats across source countries.
5. Handled compact numeric date formats from the USA file.
6. Calculated member age using date of birth.
7. Calculated the number of days since the last flight.
8. Created a stale-member indicator.
9. Applied data-quality validations.

#### Derived Columns

**Age**

Calculated based on the member's date of birth and the processing date.

**Stale Member**

A member was classified as stale when the last flight date was more than 90 days before the processing date.

#### Data Validation Rules

- Member ID should not be null.
- Member Name should not be null.
- Invalid non-empty DOB values should be rejected.
- Future DOB values should be rejected.
- Tier Code should be valid.
- Enrollment Date should be valid.
- Flight Date should be valid.
- Future enrollment and flight dates should be rejected.

DOB was treated as optional because the USA source did not contain a DOB column.

#### Silver Output

```text
/content/skypoints/data/silver/members
```

**Valid records: 8**

### Step 5: Quarantine Layer – Invalid Records

Records that failed the Silver validation rules were separated into a quarantine dataset.

This prevents invalid records from entering the Gold layer.

One invalid Australian record was identified:

- Country: Australia
- Member ID: 2
- Member Name: Jonnathan
- Invalid Enrollment Date: 2021-13-13
- Validation Error: Missing or Invalid Enrollment Date

Quarantine output:

```text
/content/skypoints/data/quarantine/invalid_members
```

**Quarantined records: 1**

### Step 6: Gold Layer – Country-Specific Tables

The valid Silver records were separated into country-specific Gold datasets.

Created the following Delta tables:

**India Gold**

```text
/content/skypoints/data/gold/gold_ind_members
```

Records: 3

**USA Gold**

```text
/content/skypoints/data/gold/gold_usa_members
```

Records: 3

**Australia Gold**

```text
/content/skypoints/data/gold/gold_aus_members
```

Records: 2

The Gold datasets contain standardized member information, derived age, flight information, and stale-member indicators.

Member IDs were not globally deduplicated because the source files contained the same IDs with different member names across countries. A confirmed business identity and country-transfer rule is required before applying global deduplication.

### Step 7: JSON Redemption Feed Processing

Processed the nested JSON redemption feed containing member information and multiple redemption transactions.

The JSON included:

- member_id
- feed_date
- redemptions
  - txn_id
  - txn_date
  - partner
  - miles_redeemed
  - status

Read the JSON using PySpark:

```python
redemption_df = (
    spark.read
    .option("multiLine", True)
    .json(json_path)
)
```

### Step 8: Flatten Nested JSON

Used PySpark `explode()` to convert the nested redemption array into individual transaction records.

```python
from pyspark.sql import functions as F

flattened_df = redemption_df.select(
    F.col("member_id"),
    F.col("feed_date"),
    F.explode("redemptions").alias("redemption")
)

redemption_flat_df = flattened_df.select(
    "member_id",
    "feed_date",
    F.col("redemption.txn_id").alias("txn_id"),
    F.col("redemption.txn_date").alias("txn_date"),
    F.col("redemption.partner").alias("partner"),
    F.col("redemption.miles_redeemed").alias("miles_redeemed"),
    F.col("redemption.status").alias("status")
)
```

The nested JSON was successfully flattened into two transaction records.

### Step 9: Join Redemption Data with Member Profiles

Created a member staging DataFrame from the supplied staging sample.

Joined the flattened redemption transaction data with the member staging data using Member ID.

Used a LEFT JOIN to retain redemption transactions even when a corresponding member profile is unavailable.

The resulting dataset included:

- Member ID
- Member Name
- Tier
- Country
- Feed Date
- Transaction ID
- Transaction Date
- Partner
- Miles Redeemed
- Transaction Status

Both redemption transactions successfully matched with the member profile for Elena.

### Step 10: Gold Redemption Table

Stored the final flattened and enriched redemption transactions as a Delta dataset.

```text
/content/skypoints/data/gold/gold_redemptions
```

**Gold redemption records: 2**

### Step 11: Source-to-Target Data Validation

Implemented source-to-target validation using PySpark.

The following checks were performed:

1. Source record count validation.
2. Distinct matched member count.
3. Redemption transaction count.
4. Unmatched member validation.
5. Duplicate transaction validation.
6. Invalid redemption amount validation.

Validation results:

| Validation | Result |
|---|---:|
| Member source records | 5 |
| Distinct matched members | 1 |
| Redemption transactions | 2 |
| Unmatched redemption records | 0 |
| Duplicate transactions | 0 |
| Invalid redemption amounts | 0 |

All executed source-to-target validation checks passed.

Note: The member staging sample contains five members, while the redemption feed references only one of them. This is expected for the supplied sample data.

### Step 12: Latest Record Wins Logic

Demonstrated the latest-record-wins approach using PySpark Window functions.

The objective is to retain the latest member record when a member changes countries.

Used:

- `Window.partitionBy()`
- `orderBy()`
- `row_number()`

Records were ordered by source update timestamp in descending order, and the latest record was retained.

This was demonstrated using illustrative sample records with timestamps. Actual country-transfer processing requires a reliable source update timestamp or an approved business rule.

### Step 13: Automated Testing with PyTest

Prepared PyTest test cases for the ETL transformation logic.

The planned test coverage includes:

1. JSON flattening validation.
2. Redemption data-quality validation.
3. Latest-record-wins validation.

PyTest is used to automate the validation of transformation logic and reduce manual testing.

### Step 14: Azure Production Architecture

The proposed production architecture uses:

- Azure Data Factory for orchestration.
- Azure Data Lake Storage Gen2 for landing and Delta data storage.
- Azure Databricks for PySpark processing.
- Delta Lake for Bronze, Silver, and Gold datasets.
- Azure Monitor and Log Analytics for monitoring.

Proposed ADF pipeline execution:

1. Schedule Trigger.
2. Get Metadata activity.
3. Copy Data activity.
4. Databricks Notebook activity for Bronze and Silver processing.
5. Data validation activity.
6. Databricks Notebook activity for Gold processing.
7. If Condition activity to publish data only when validations pass.
8. Logging and failure notifications.

This is the proposed production architecture; actual Azure resource deployment and ADF execution are separate from the Colab implementation.

## 6. Final Pipeline Execution Results

The final reconciliation produced the following results:

| Layer / Dataset | Record Count |
|---|---:|
| Bronze | 9 |
| Silver Valid | 8 |
| Quarantine | 1 |
| India Gold | 3 |
| USA Gold | 3 |
| Australia Gold | 2 |
| Gold Redemptions | 2 |

All final pipeline record-count reconciliation checks passed.

## 7. Project Folder Structure

Recommended repository structure:

```text
skypoints-data-engineering-assessment/
│
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_transformation.ipynb
│   └── 03_gold_redemptions.ipynb
│
├── tests/
│   └── test_etl.py
│
├── README.md
├── requirements.txt
└── .gitignore
```

The notebook names above are suggested names. Replace them with the actual notebook names used in the repository.

## 8. How to Execute the Project

### Prerequisites
- Python 3.11
- Java compatible with PySpark
- Required Python dependencies

### Step 1: Clone the repository

```bash
git clone https://github.com/shruti-nikumbh-git/skypoints-data-engineering-assessment.git
```

### Step 2: Navigate to the project folder

```bash
cd skypoints-data-engineering-assessment
```

### Step 3: Install dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Open the notebooks

Open the three project notebooks using Google Colab or Jupyter Notebook.

### Step 5: Upload source files

Upload the required input files:
- IND.xlsx
- USA.xlsx
- AUS.xlsx

Also provide the redemption JSON input used in the notebook.

### Step 6: Execute the notebooks

Execute the notebooks in the required order:

1. Bronze ingestion
2. Silver transformation and data validation
3. Gold processing and redemption transactions

### Step 7: Verify the output

Check the final record counts and validation results against the results documented above.

### Step 8: Run automated tests

If the PyTest test file is included in the repository, execute:

```bash
pytest -v tests/
```

**Note:** File paths may need to be updated according to your local or Colab environment. The Azure architecture is proposed; the demonstrated execution was performed in Google Colab.
