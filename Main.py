import pandas as pd
import os
import glob
from sqlalchemy import create_engine, text

# -- All Data Paths --
Folder_path = r"D:\Shriyash\Projects\Billing and Stock Allocation\Data"
FBA_Shipments_path = os.path.join(Folder_path, "FBA Shipments.csv")
FBA_Sale_and_Inventory_Report_path = os.path.join(Folder_path, "FBA Sale & Inventory Report.xlsx")
Viability_sheet_path = os.path.join(Folder_path, "Viability Sheet.xlsx")
All_Orders_path = os.path.join(Folder_path, "All Orders.txt")
output_excel_path = os.path.join(os.path.dirname(Folder_path), "Output.xlsx")  # one level up from Folder_path

# -- Function to get SQL data and create a dataframe --
def get_sql_data(state_name):
    try:
        engine = create_engine("mysql+pymysql://"DB_USER":"DB_PASSWORD"@localhost/State_Stock_Data")
        with engine.connect() as connection:
            query = f"SELECT * FROM `{state_name}_data`"
            df = pd.read_sql(text(query), connection)
        return df
    except Exception as e:
        print("Database Error:", e)
        return None

# -- Function to allocate stock against order ids --
def allocate_stock(df, order_col="Amazon Order Id", sku_col="Merchant SKU", required_col="Shipped Quantity", available_col="Stock", scancode_col="Scancode"):
    """
    Allocate stock per (Order ID + SKU) level, using stock from multiple scancodes, but stop once SKU requirement is fulfilled.

    Args:
    - df: Input dataframe with orders and stock availability.
    - order_col: Column for Order ID.
    - sku_col: Column for unique product identifier (e.g., green/blue ball)
    - required_col: Quantity that needs to be shipped.
    - available_col: Available stock for allocation.
    - scancode_col: Barcode or pack-level identifier used for stock

    Returns:
    - DataFrame with 'Allocated Qty' column and shortage rows for unfulfilled items.
    """

    df["Allocated Qty"] = 0

    # -- Track remaining required quantity per (Order ID + SKU) --
    required_quantity_tracker = {}

    # -- Track total stock left per scancode --
    stock_remaining = {}

    for idx, row in df.iterrows():
        order_id = row[order_col]
        sku = row[sku_col]
        scancode = row[scancode_col]
        key = (order_id, sku)
        available = row[available_col]

        # -- Initialize required quantity once per (Order ID, SKU) --
        if key not in required_quantity_tracker:
            required_quantity_tracker[key] = row[required_col]

        required = required_quantity_tracker[key]

        # -- Skip if already fulfilled --
        if required <= 0:
            continue

        # -- Initialize stock for the scancode if not already tracked --
        if scancode not in stock_remaining:
            stock_remaining[scancode] = available

        # -- If stock is 0 or less, no allocation --
        if stock_remaining[scancode] <= 0:
            continue

        # -- Allocate stock --
        allocatable = min(required, stock_remaining[scancode])
        df.at[idx, "Allocated Qty"] = allocatable
        stock_remaining[scancode] -= allocatable
        required_quantity_tracker[key] -= allocatable

    # -- Create shortage rows for any unfulfilled (Order ID, SKU) combinations --
    shortage_rows = []
    for (order_id, sku), shortfall in required_quantity_tracker.items():
        if shortfall > 0:
            matching_rows = df[(df[order_col] == order_id) & (df[sku_col] == sku)]
            if not matching_rows.empty:
                last_idx = matching_rows.index[-1]
                shortage_row = df.loc[last_idx].copy()
                shortage_row["Allocated Qty"] = -shortfall
                shortage_row[available_col] = 0
                shortage_rows.append(shortage_row)
                
    # -- If shortage detected then add that row to errors df --
    if shortage_rows:
        df = pd.concat([df, pd.DataFrame(shortage_rows)], ignore_index=True)

    return df

# -- Function to get problematic rows, delete them from main dataframe and generate a dataframe containing errors --
def split_errors(df):
    """
    Splits the dataframe into:
    - 'clean_df': rows without errors
    - 'errors_df': rows with errors

    Error conditions:
    - NetAmt == 0
    - OR Discount < 0
    - OR Allocated Qty < 0
    - OR Allocated Qty is NaN
    - OR NetAmt is NaN
    - OR Discount is NaN
    """
    # Define error condition
    error_condition = (
        (df["NetAmt"] == 0) |
        (df["Discount"] < 0) |
        (df["Allocated Qty"] < 0) |
        (df["Allocated Qty"].isna()) |
        (df["NetAmt"].isna()) |
        (df["Discount"].isna())
    )

    # Get errors
    errors_df = df[error_condition].copy()

    # Get clean data (opposite of error condition)
    clean_df = df[~error_condition].copy()

    return clean_df, errors_df

# -- Generating dataframe for FBA Shipments file --
df_FBA_shipments = pd.read_csv(FBA_Shipments_path)

# -- Removing rows with value "XHJW" in FC column --
df_FBA_shipments = df_FBA_shipments[df_FBA_shipments["FC"] != "XHJW"]

# -- Concatenating columns "Amazon Order Id", "Merchant SKU", "Shipped Quantity", "FC" to generate column "Merchant Order Id" for df_FBA_shipments --
df_FBA_shipments["Merchant Order Id"] = df_FBA_shipments["Amazon Order Id"].astype(str) + df_FBA_shipments["Merchant SKU"].astype(str) + df_FBA_shipments["Shipped Quantity"].astype(str) + df_FBA_shipments["FC"].astype(str)

# -- Generating data frame for FBA_Sale_and_Inventory_Report file Sheet "Amz fulfilled shipments" --
df_FBA_Sale_and_Inventory_Report = pd.read_excel(FBA_Sale_and_Inventory_Report_path, sheet_name="Amz fulfilled shipments")

# -- Concatenating columns "Amazon Order Id", "SKU", "Shipped Quantity", "FC" to generate column "Merchant Order Id" for df_FBA_Sale_and_Inventory_Report --
df_FBA_Sale_and_Inventory_Report["Merchant Order Id"] = df_FBA_Sale_and_Inventory_Report["Amazon Order Id"].astype(str) + df_FBA_Sale_and_Inventory_Report["SKU"].astype(str) + df_FBA_Sale_and_Inventory_Report["Shipped Quantity"].astype(str) + df_FBA_Sale_and_Inventory_Report["FC"].astype(str)

# -- Removing matching columns (df_FBA_Sale_and_Inventory_Report) from df_FBA_shipments  --
df_FBA_shipments = df_FBA_shipments[~df_FBA_shipments["Merchant Order Id"].isin(df_FBA_Sale_and_Inventory_Report["Merchant Order Id"])]

# -- Setting values of column "Merchant Order Id" to "" --
df_FBA_shipments["Merchant Order Id"] = ""

# -- Selecting only required columns from FBA Shipments --
df_FBA_shipments = df_FBA_shipments[["Amazon Order Id", "Merchant SKU", "Shipped Quantity", "FC"]]

# -- Generating dataframe for Viability sheet --
df_viability = pd.read_excel(Viability_sheet_path)

# -- Adding "MOQ" and "Modi SKU" columns from Viability dataframe into FBA Shipments on "Merchant SKU" = "SKU" --
df_FBA_shipments = df_FBA_shipments.merge(
    df_viability[["SKU", "MOQ", "Modi SKU"]],
    left_on="Merchant SKU",
    right_on="SKU",
    how="left"
).drop(columns=["SKU"])

# -- Overwriting column "Shipped Quantity" by multiplying it with "MOQ" --
df_FBA_shipments["Shipped Quantity"] = df_FBA_shipments["Shipped Quantity"] * df_FBA_shipments["MOQ"] 

# -- Generating dataframe for B2C file --
file_pattern = "GST_MTR_B2C*.csv"

# -- Search for files matching the pattern --
matching_files = glob.glob(os.path.join(Folder_path, file_pattern))
if matching_files:
    # -- If files exist, pick the first matching file --
    file_to_load = matching_files[0]
    df_b2c = pd.read_csv(file_to_load)

    # -- First, make sure df_b2c has only one row per 'Order Id' --
    df_b2c = df_b2c.drop_duplicates(subset=["Order Id"], keep="first")

    # -- Getting "Ship To State" column from B2C file on "Order Id" --
    df_FBA_shipments = df_FBA_shipments.merge(
        df_b2c[["Order Id", "Ship To State"]],
        left_on="Amazon Order Id",
        right_on="Order Id",
        how="left"
    ).drop(columns=["Order Id"])

else:
    print("No matching file found.")

# -- Generating dataframe for B2B file --
file_pattern = "GST_MTR_B2B*.csv"

# -- Search for files matching the pattern --
matching_files = glob.glob(os.path.join(Folder_path, file_pattern))
if matching_files:
    # -- If files exist, pick the first matching file --
    file_to_load = matching_files[0]
    df_b2b = pd.read_csv(file_to_load)

    # -- First, make sure df_b2b has only one row per 'Order Id' --
    df_b2b = df_b2b.drop_duplicates(subset=["Order Id"], keep="first")

    # -- Getting "Ship To State" column from B2C file on "Order Id" --
    df_FBA_shipments = df_FBA_shipments.merge(
        df_b2b[["Order Id", "Bill To State"]],
        left_on="Amazon Order Id",
        right_on="Order Id",
        how="left"
    ).drop(columns=["Order Id"])

else:
    print("No matching file found.")

# -- Merging columns "Ship To State" and "Bill To State" columns to fill blank data in "Ship To State" --
df_FBA_shipments["Ship To State"] = df_FBA_shipments["Ship To State"].fillna(df_FBA_shipments["Bill To State"])

# -- Dropping "Bill To State" column --
df_FBA_shipments = df_FBA_shipments.drop(columns=["Bill To State"])

# -- Generating dataframe for All Orders file --
df_All_Orders = pd.read_csv(All_Orders_path, delimiter='\t')

# -- Removing rows that do not contain value "FBA" in column "sku" --> All Orders -- 
df_All_Orders = df_All_Orders[df_All_Orders["sku"].str.contains("FBA", na=False)]

# -- Filtering "item-status" column to only include "Shipping" and "Shipped" values --
df_All_Orders = df_All_Orders[df_All_Orders["item-status"].isin(["Shipping", "Shipped"])]

# -- Creating NetAmt Column as "item-price" + "shipping-price" + "gift-wrap-price" - "item-promotion-discount" - "ship-promotion-discount" --
columns_to_convert = ["item-price", "shipping-price", "gift-wrap-price", "item-promotion-discount", "ship-promotion-discount"]

for col in columns_to_convert:
    df_All_Orders[col] = pd.to_numeric(df_All_Orders[col], errors='coerce').fillna(0)

df_All_Orders["NetAmt"] = (df_All_Orders["item-price"] + df_All_Orders["shipping-price"] + df_All_Orders["gift-wrap-price"] - df_All_Orders["item-promotion-discount"] - df_All_Orders["ship-promotion-discount"])  

# -- Getting "NetAmt" column from All Orders file on "amazon-order-id" and "sku" --
df_FBA_shipments = df_FBA_shipments.merge(
    df_All_Orders[["amazon-order-id", "sku", "NetAmt"]],
    left_on=["Amazon Order Id", "Merchant SKU"],
    right_on=["amazon-order-id", "sku"],
    how="left"
).drop(columns=["amazon-order-id", "sku"])

# -- Assigning Values to "code1" and "code2" columns --

# -- Mapping fallback values based on FC prefix --
region_mapping_code1 = {
    ("BOM", "PNQ"): "AMAZON(MAHARASHTRA)",
    ("AMD",): "AMAZON(GUJARAT)",
    ("DEL",): "AMAZON(HARYANA)",
    ("BLR",): "AMAZON(KARNATAKA)",
}

region_mapping_code2 = {
    ("BOM", "PNQ"): "1234567890",
    ("AMD",): "1234567894",
    ("DEL",): "1234567893",
    ("BLR",): "1234567892",
}

# -- Define function to assign code1 --
def assign_code1(row):
    fc = str(row["FC"])
    state = str(row["Ship To State"]).upper()

    for prefixes, code in region_mapping_code1.items():
        if any(fc.startswith(prefix) for prefix in prefixes):
            if (prefixes == ("BOM", "PNQ") and state == "MAHARASHTRA") or \
               (prefixes == ("AMD",) and state == "GUJARAT") or \
               (prefixes == ("DEL",) and state == "HARYANA") or \
               (prefixes == ("BLR",) and state == "KARNATAKA"):
                return code
    return "AMAZON(OMS)"

# -- Define function to assign code2 --
def assign_code2(row):
    fc = str(row["FC"])
    state = str(row["Ship To State"]).upper()

    for prefixes, code in region_mapping_code2.items():
        if any(fc.startswith(prefix) for prefix in prefixes):
            if (prefixes == ("BOM", "PNQ") and state == "MAHARASHTRA") or \
               (prefixes == ("AMD",) and state == "GUJARAT") or \
               (prefixes == ("DEL",) and state == "HARYANA") or \
               (prefixes == ("BLR",) and state == "KARNATAKA"):
                return code
    return "1234567891"

# -- Apply logic to dataframe --
df_FBA_shipments["code1"] = df_FBA_shipments.apply(assign_code1, axis=1)
df_FBA_shipments["code2"] = df_FBA_shipments.apply(assign_code2, axis=1)

# -- Grouping duplicate "Amazon Order Id", "Merchant SKU" columns and adding their row values for columns ["Shipped Quantity"] --
sum_columns = ["Shipped Quantity"]
group_columns = ["Amazon Order Id", "Merchant SKU"]

# -- All other columns – takes the first value for each group --
first_columns = [col for col in df_FBA_shipments.columns if col not in sum_columns + group_columns]

# -- Building aggregation dictionary --
agg_dict = {col: "sum" for col in sum_columns}
agg_dict.update({col: "first" for col in first_columns})

# -- Group and aggregate -- 
df_FBA_shipments = df_FBA_shipments.groupby(group_columns, as_index=False).agg(agg_dict)

# -- Creating regional sheets based on FC codes --
df_MAHARASHTRA = df_FBA_shipments[df_FBA_shipments["FC"].str.startswith(("PNQ", "BOM"))]
df_KARNATAKA = df_FBA_shipments[df_FBA_shipments["FC"].str.startswith("BLR")]
df_GUJARAT = df_FBA_shipments[df_FBA_shipments["FC"].str.startswith("AMD")]
df_HARYANA = df_FBA_shipments[df_FBA_shipments["FC"].str.startswith("DEL")]

# -- Generating a dataframe for Maharashtra Stock data --
df_maharashtra_data = get_sql_data("Maharashtra")

# -- Assigning "Sc_Code", "Sc_MRP", "Spt_CreditQty" --
df_MAHARASHTRA = df_MAHARASHTRA.merge(
    df_maharashtra_data[["MODI_ SKU", "Scancode", "MRP", "Stock"]],
    left_on = ["Modi SKU"],
    right_on = ["MODI_ SKU"],
    how = "left"
).drop(columns = ["MODI_ SKU"])

# -- Using Allocate function to allocate stock --
df_MAHARASHTRA = allocate_stock(df_MAHARASHTRA)

# -- Dropping rows where allocated value is 0 --
df_MAHARASHTRA = df_MAHARASHTRA[df_MAHARASHTRA["Allocated Qty"] != 0] 

# -- Generating a dataframe for Karnataka Stock data --
df_karnataka_data = get_sql_data("Karnataka")

# -- Assigning "Sc_Code", "Sc_MRP", "Spt_CreditQty" --
df_KARNATAKA = df_KARNATAKA.merge(
    df_karnataka_data[["MODI_ SKU", "Scancode", "MRP", "Stock"]],
    left_on = ["Modi SKU"],
    right_on = ["MODI_ SKU"],
    how = "left"
).drop(columns = ["MODI_ SKU"])

# -- Using Allocate function to allocate stock --
df_KARNATAKA = allocate_stock(df_KARNATAKA)

# -- Dropping rows where allocated value is 0 --
df_KARNATAKA = df_KARNATAKA[df_KARNATAKA["Allocated Qty"] != 0]

# -- Generating a dataframe for Gujarat Stock data --
df_gujarat_data = get_sql_data("Gujarat")

# -- Assigning "Sc_Code", "Sc_MRP", "Spt_CreditQty" --
df_GUJARAT = df_GUJARAT.merge(
    df_gujarat_data[["MODI_ SKU", "Scancode", "MRP", "Stock"]],
    left_on = ["Modi SKU"],
    right_on = ["MODI_ SKU"],
    how = "left"
).drop(columns = ["MODI_ SKU"])

# -- Using Allocate function to allocate stock --
df_GUJARAT = allocate_stock(df_GUJARAT)

# -- Dropping rows where allocated value is 0 --
df_GUJARAT = df_GUJARAT[df_GUJARAT["Allocated Qty"] != 0]

# -- Generating a dataframe for Haryana Stock data --
df_haryana_data = get_sql_data("Haryana")

# -- Assigning "Sc_Code", "Sc_MRP", "Spt_CreditQty" --
df_HARYANA = df_HARYANA.merge(
    df_haryana_data[["MODI_ SKU", "Scancode", "MRP", "Stock"]],
    left_on = ["Modi SKU"],
    right_on = ["MODI_ SKU"],
    how = "left"
).drop(columns = ["MODI_ SKU"])

# -- Using Allocate function to allocate stock --
df_HARYANA = allocate_stock(df_HARYANA)

# -- Dropping rows where allocated value is 0 --
df_HARYANA = df_HARYANA[df_HARYANA["Allocated Qty"] != 0]

# -- Setting correct "NetAmt" values based on Scancode quantity for all Statewise dataframes --
df_MAHARASHTRA["NetAmt"] = df_MAHARASHTRA["NetAmt"] * df_MAHARASHTRA["Allocated Qty"] / df_MAHARASHTRA["Shipped Quantity"]
df_KARNATAKA["NetAmt"] = df_KARNATAKA["NetAmt"] * df_KARNATAKA["Allocated Qty"] / df_KARNATAKA["Shipped Quantity"]
df_GUJARAT["NetAmt"] = df_GUJARAT["NetAmt"] * df_GUJARAT["Allocated Qty"] / df_GUJARAT["Shipped Quantity"]
df_HARYANA["NetAmt"] = df_HARYANA["NetAmt"] * df_HARYANA["Allocated Qty"] / df_HARYANA["Shipped Quantity"]

# -- Populating "MRP*Quantity" value for all Statewise dataframes --
df_MAHARASHTRA["MRP*Quantity"] = df_MAHARASHTRA["MRP"] * df_MAHARASHTRA["Allocated Qty"]
df_KARNATAKA["MRP*Quantity"] = df_KARNATAKA["MRP"] * df_KARNATAKA["Allocated Qty"]
df_GUJARAT["MRP*Quantity"] = df_GUJARAT["MRP"] * df_GUJARAT["Allocated Qty"]
df_HARYANA["MRP*Quantity"] = df_HARYANA["MRP"] * df_HARYANA["Allocated Qty"]

# -- Calculating Discount for all Statewise dataframes --
df_MAHARASHTRA["Discount"] = df_MAHARASHTRA["MRP*Quantity"] - df_MAHARASHTRA["NetAmt"]
df_KARNATAKA["Discount"] = df_KARNATAKA["MRP*Quantity"] - df_KARNATAKA["NetAmt"] 
df_GUJARAT["Discount"] = df_GUJARAT["MRP*Quantity"] - df_GUJARAT["NetAmt"]
df_HARYANA["Discount"] = df_HARYANA["MRP*Quantity"] - df_HARYANA["NetAmt"]

# -- Dropping columns "Stock" --
df_MAHARASHTRA = df_MAHARASHTRA.drop(columns=["Stock"])
df_KARNATAKA = df_KARNATAKA.drop(columns=["Stock"])
df_GUJARAT = df_GUJARAT.drop(columns=["Stock"])
df_HARYANA = df_HARYANA.drop(columns=["Stock"])

# -- Creating a dataframes that will include all the errors --
# -- Running the fuction for each Statewise dataframe --
df_MAHARASHTRA, errors_maharashtra = split_errors(df_MAHARASHTRA)
df_KARNATAKA, errors_karnataka = split_errors(df_KARNATAKA)
df_GUJARAT, errors_gujarat = split_errors(df_GUJARAT)
df_HARYANA, errors_haryana = split_errors(df_HARYANA)

# -- Combine all error DataFrames into one --
df_combined_errors = pd.concat(
    [errors_maharashtra, errors_karnataka, errors_gujarat, errors_haryana],
    ignore_index=True
)

# -- Exporting dataframes as excel sheets --
try:
    with pd.ExcelWriter(output_excel_path, engine="openpyxl") as writer:
        df_FBA_shipments.to_excel(writer, sheet_name="Today's FBA Shipments", index=False)
        df_MAHARASHTRA.to_excel(writer, sheet_name="Maharashtra", index=False)
        df_KARNATAKA.to_excel(writer, sheet_name="KARNATAKA", index=False)
        df_GUJARAT.to_excel(writer, sheet_name="GUJARAT", index=False)
        df_HARYANA.to_excel(writer, sheet_name="HARYANA", index=False)
        df_combined_errors.to_excel(writer, sheet_name="Errors", index=False)
    print("Dataframes save successfully")
except Exception as e:
    print("Error: ", e)
