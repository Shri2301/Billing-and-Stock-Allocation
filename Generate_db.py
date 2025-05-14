import pandas as pd
from sqlalchemy import create_engine, text

# MySQL connection string (without the database part)
engine = create_engine("mysql+pymysql://"DB_USER":"DB_PASSWORD"@localhost/")

# Create database if not exists
with engine.connect() as connection:
    connection.execute(text("CREATE DATABASE IF NOT EXISTS state_stock_data"))

# Now, connect to the created database
engine = create_engine("mysql+pymysql://"DB_USER":"DB_PASSWORD"@localhost/state_stock_data")

# Read Excel

file_path = r"D:\Shriyash\Projects\Billing and Stock Allocation\Data\SQL Data.xlsx"

df_maharashtra = pd.read_excel(file_path, sheet_name="Maharashtra Data")
df_karnataka = pd.read_excel(file_path, sheet_name="Karnataka Data")
df_gujurat = pd.read_excel(file_path, sheet_name="Gujarat Data")
df_haryana = pd.read_excel(file_path, sheet_name="Haryana Data")

# Push dataframe to MySQL
df_maharashtra.to_sql("maharashtra_data", con=engine, index=False, if_exists='replace')
df_karnataka.to_sql("karnataka_data", con=engine, index=False, if_exists='replace')
df_gujurat.to_sql("gujurat_data", con=engine, index=False, if_exists='replace')
df_haryana.to_sql("haryana_data", con=engine, index=False, if_exists='replace')
