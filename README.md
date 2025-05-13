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

**Note:** This repository contains a **demo version** of a project originally developed for internal use at a company.  
All data, folder structures, and database connections have been **mocked or sanitized** to make it suitable for public release.

This demo replicates the logic and workflow using:
- Sample FBA shipment and GST reports
- Local database tables 
- Modified folder paths and filenames for local execution

All sensitive or proprietary elements have been removed or replaced with publicly shareable placeholders.



