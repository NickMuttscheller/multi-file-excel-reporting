# Multi-File Excel Reporting Automation

## Overview

This project is a production-style Python reporting pipeline designed to automate the consolidation, cleaning, and analysis of multiple Excel and CSV sales files.

It simulates a real-world business scenario where sales data is exported from different systems and dropped into a folder. The pipeline processes all files, applies data validation and cleaning, and produces a professional Excel reporting package.

---

## Business Use Case

Companies often receive sales data from multiple sources:

- Different departments
- Different regions
- Different export formats (CSV, Excel)
- Inconsistent data structures

This creates challenges such as:

- Duplicate records
- Missing or invalid values
- Inconsistent formatting
- Manual reporting effort

This solution automates the entire workflow:

Drop files → Run script → Get clean, structured, professional reports

---

## Features

### Multi-File Processing
- Automatically scans the input/ folder
- Supports .csv, .xlsx, .xls
- Skips unsupported files gracefully
- Logs all processed and skipped files

### Data Cleaning & Standardization
- Normalizes column names across files
- Removes fully empty rows
- Trims whitespace and fixes formatting
- Standardizes text fields (regions, names, products)
- Converts and validates dates
- Converts numeric fields safely
- Recalculates revenue (units_sold * unit_price)
- Removes duplicates
- Filters invalid records

### Data Validation
- Detects invalid dates, numeric values, and missing required fields
- Separates rejected rows into a data quality report
- Tracks all issues for transparency

### KPI Reporting
- Total revenue
- Total units sold
- Total transactions
- Average revenue per transaction
- Revenue by region
- Revenue by product
- Revenue by category
- Revenue by salesperson
- Monthly revenue trend
- Top-performing region, product, and salesperson

### Excel Report Generation
Creates a professional multi-sheet Excel workbook:

- Cleaned Master Data
- KPI Summary
- Aggregations
- File Processing Summary
- Charts

Includes:
- Styled headers
- Auto column sizing
- Currency formatting
- Freeze panes

### Charts
- Revenue by Region (Bar Chart)
- Revenue by Product (Bar Chart)
- Monthly Revenue Trend (Line Chart)

### Logging System
Production-style logging covering:
- File discovery
- Processing steps
- Cleaning actions
- Errors with traceback
- Output generation

Log file location:
log/reporting_run.log

---

## Project Structure

scripts/
└── multi_file_excel_reporting/
    ├── input/
    ├── output/
    ├── log/
    ├── README.md
    └── reporting_pipeline.py

---

## Input Data Requirements

Supported formats:
- CSV (.csv)
- Excel (.xlsx, .xls)

Expected fields (flexible naming supported):

- Required fields:
  - date
  - region
  - salesperson
  - customer_name
  - product
  - category
  - units_sold
  - unit_price

- Optional fields:
  - revenue (automatically calculated if missing)
  - status

The script handles messy real-world data such as:
- Missing values
- Extra spaces
- Inconsistent casing
- Invalid numbers
- Duplicate rows
- Slight schema differences

---

## Example Workflow

1. Place all sales files into:
input/

2. Run the script:
python reporting_pipeline.py

3. Outputs will be generated in:
output/

---

## Output Files

Excel Reporting Pack:
output/sales_reporting_pack.xlsx

Includes:
- Cleaned Master Data
- KPI Summary
- Aggregations
- File Processing Summary
- Charts

Cleaned Dataset:
output/cleaned_master_data.csv

Data Quality Report:
output/data_quality_issues.csv (only generated if invalid rows are found)

Processing Summary:
output/processing_summary.txt

---

## Data Cleaning Logic

The pipeline performs:
- Column normalization (with alias support)
- Null handling
- Text standardization
- Date parsing with error handling
- Numeric conversion using safe coercion
- Revenue recalculation
- Duplicate removal
- Invalid row filtering

Only valid records are used for reporting.

---

## Validation Logic

Each record is validated for:
- Required fields
- Valid dates
- Valid numeric values
- Non-negative quantities and prices

Invalid rows are:
- Removed from final dataset
- Stored in a separate report

---

## Technology Stack

- Python
- pandas
- openpyxl
- pathlib
- logging

---

## How to Run

cd scripts/multi_file_excel_reporting
python reporting_pipeline.py

---

## Portfolio Value

This project demonstrates:

- Real-world data cleaning and validation
- Multi-file ingestion pipelines
- Business KPI reporting
- Excel automation with formatting and charts
- Production-style logging and error handling
- Modular and maintainable code structure

It reflects the type of automation work delivered in freelance data and reporting projects.

---

## Notes

- Designed to handle inconsistent business data
- Easily extendable for additional KPIs
- Suitable for sales, finance, and operational reporting workflows