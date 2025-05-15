# Billing-Automation-for-FBA (Fulfilled By Amazon)

# Project Overview
This project was originally developed as an internal automation tool to handle end-to-end processing of Amazon FBA shipment and inventory data. It automated the process of billing, stock allocation, and error detection using data from Amazon FBA shipments, inventory reports, GST filings, and a SQL database. It helped streamline order management, validate data, allocate stock intelligently, and flag problematic entries.

# Features
- Reads and cleans Amazon FBA shipment data.
- Compares against actual fulfilled shipments to remove duplicates.
- Merges product metadata (MOQ, Modified SKU) from a viability sheet.
- Multiplies quantity by MOQ for accurate stock planning.
- Extracts "Ship To State" from GST B2C data.
- Connects to a MySQL database to get available stock by state.
- Allocates stock intelligently per `(Order ID, SKU)` using multiple scancodes.
- Flags and separates problematic entries (zero net amount, invalid discounts, allocation issues).
- Outputs clean and error data into a structured Excel file.

# Tech Stack
- Python (pandas, openpyxl, mysql-connector)
- MySQL (local/mock database)
- Excel (xlsx) input/output handling

**Note:** This repository contains a **demo version** of a project originally developed for internal use at a company.  
All data, folder structures, and database connections have been **mocked or sanitized** to make it suitable for public release.

This demo replicates the logic and workflow using:
- Sample FBA shipment and GST reports
- Local database tables 
- Modified folder paths and filenames for local execution

All sensitive or proprietary elements have been removed or replaced with publicly shareable placeholders.

**Steps to run this project:**
- Prerequisites
  Python 3.8+. Make sure Python is installed. You can download it from python.org.
- Clone the Repository
  ```bash
  git clone <your-repository-url>
- Navigate to the project folder
  ```bash
  cd .\Billing_and_Stock_Allocation\
- Install Dependencies
  ```bash
  pip install -r requirements.txt
- Create local database
  In `Generate_db.py` replace "DB_USER" and "DB_PASSWORD" in `Generate_db.py` with your <db_user> and <db_password> at lines 5 & 12. Replace the `file_path` to the respective local path at line 16. 
  Run the script.
  ```bash
  python Generate_db.py
- Make changes in main script
  In `Main.py` replace "DB_USER" and "DB_PASSWORD" with your <db_user> and <db_password> at line 17 and replace `Folder_path` with your local <Data Folder> path at line 7.
- Run main script
  ```bash
  python Main.py
- Output is stored in "Output.xlsx" file