"""
===============================================================================
Python Script: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This script performs the ETL (Extract, Transform, Load) process to 
    populate the Silver layer tables from the Bronze layer using pandas.
    
Actions Performed:
    - Reads data from Bronze layer CSV/Parquet files
    - Applies transformations and data cleansing
    - Writes transformed data to Silver layer files

Usage Example:
    python load_silver.py
===============================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys

# Add these lines to debug
print("=" * 80)
print("DEBUG INFO")
print("=" * 80)
print(f"Current working directory: {os.getcwd()}")
print(f"Script is looking for files in: {os.path.abspath('./data/bronze/')}")
print(f"Does bronze folder exist? {os.path.exists('./data/bronze/')}")
if os.path.exists('./data/bronze/'):
    print(f"Files in bronze folder: {os.listdir('./data/bronze/')}")
print("=" * 80)

# Configuration
BRONZE_PATH = './data/bronze/'
SILVER_PATH = './data/silver/'

def print_header(message, char='='):
    """Print formatted header message"""
    print(f"\n{char * 80}")
    print(message)
    print(f"{char * 80}")

def print_subheader(message):
    """Print formatted subheader message"""
    print(f"\n{'-' * 80}")
    print(message)
    print(f"{'-' * 80}")

def print_step(message):
    """Print step message"""
    print(f">> {message}")

def check_environment():
    """Check if the environment is set up correctly"""
    print_header('Environment Check')
    print_step(f'Current working directory: {os.getcwd()}')
    print_step(f'Bronze path: {os.path.abspath(BRONZE_PATH)}')
    print_step(f'Silver path: {os.path.abspath(SILVER_PATH)}')
    
    # Check if bronze folder exists
    if not os.path.exists(BRONZE_PATH):
        print_step(f'ERROR: Bronze folder does not exist at {os.path.abspath(BRONZE_PATH)}')
        print_step('Please create the folder and add your CSV files')
        sys.exit(1)
    
    # List files in bronze folder
    bronze_files = os.listdir(BRONZE_PATH)
    print_step(f'Files found in bronze folder: {len(bronze_files)}')
    for file in bronze_files:
        print_step(f'  - {file}')
    
    # Check for required files
    required_files = [
        'cust_info.csv',
        'prd_info.csv', 
        'sales_details.csv',
        'cust_az12.csv',
        'loc_a101.csv',
        'px_cat_g1v2.csv'
    ]
    
    missing_files = []
    for file in required_files:
        if file not in bronze_files:
            missing_files.append(file)
    
    if missing_files:
        print_step(f'WARNING: Missing {len(missing_files)} required file(s):')
        for file in missing_files:
            print_step(f'  - {file}')
        response = input('\nDo you want to continue anyway? (y/n): ')
        if response.lower() != 'y':
            sys.exit(1)
    
    print_step('Environment check passed!')
    print('=' * 80)

# ... [rest of your functions remain the same] ...

def main():
    """Main ETL execution function"""
    
    # First, check if environment is set up correctly
    check_environment()
    
    batch_start_time = datetime.now()
  

def print_header(message, char='='):
    """Print formatted header message"""
    print(f"\n{char * 80}")
    print(message)
    print(f"{char * 80}")

def print_subheader(message):
    """Print formatted subheader message"""
    print(f"\n{'-' * 80}")
    print(message)
    print(f"{'-' * 80}")

def print_step(message):
    """Print step message"""
    print(f">> {message}")

def load_bronze_data(filename, file_format='csv'):
    """Load data from bronze layer"""
    filepath = os.path.join(BRONZE_PATH, filename)
    
    if file_format == 'csv':
        return pd.read_csv(filepath)
    elif file_format == 'parquet':
        return pd.read_parquet(filepath)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

def save_silver_data(df, filename, file_format='csv'):
    # Save data to silver layer
    # Create directory if it doesn't exist
    os.makedirs(SILVER_PATH, exist_ok=True)
    
    filepath = os.path.join(SILVER_PATH, filename)
    
    if file_format == 'csv':
        df.to_csv(filepath, index=False)
    elif file_format == 'parquet':
        df.to_parquet(filepath, index=False)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

def transform_cust_info(df):
    """Transform Customer Info table"""
    # Remove rows with null customer ID
    df = df[df['cst_id'].notna()].copy()
    
    # Sort by create date and keep most recent record per customer
    df = df.sort_values('cst_create_date', ascending=False)
    df = df.drop_duplicates(subset=['cst_id'], keep='first')
    
    # Trim string fields
    df['cst_firstname'] = df['cst_firstname'].str.strip()
    df['cst_lastname'] = df['cst_lastname'].str.strip()
    
    # Normalize marital status
    df['cst_marital_status'] = df['cst_marital_status'].str.upper().str.strip()
    df['cst_marital_status'] = df['cst_marital_status'].map({
        'S': 'Single',
        'M': 'Married'
    }).fillna('n/a')
    
    # Normalize gender
    df['cst_gndr'] = df['cst_gndr'].str.upper().str.strip()
    df['cst_gndr'] = df['cst_gndr'].map({
        'F': 'Female',
        'M': 'Male'
    }).fillna('n/a')
    
    # Select and order columns
    columns = ['cst_id', 'cst_key', 'cst_firstname', 'cst_lastname', 
               'cst_marital_status', 'cst_gndr', 'cst_create_date']
    
    return df[columns].reset_index(drop=True)

def transform_prd_info(df):
    #Transform CRM Product Info table
    df = df.copy()
    
    # Extract category ID (first 5 chars, replace - with _)
    df['cat_id'] = df['prd_key'].str[:5].str.replace('-', '_')
    
    # Extract product key (from position 7 onwards, skipping position 6 which is the dash)
    df['prd_key'] = df['prd_key'].str[6:]
    
    # Fill null costs with 0
    df['prd_cost'] = df['prd_cost'].fillna(0)
    
    # Normalize product line
    df['prd_line'] = df['prd_line'].str.upper().str.strip()
    df['prd_line'] = df['prd_line'].map({
        'M': 'Mountain',
        'R': 'Road',
        'S': 'Other Sales',
        'T': 'Touring'
    }).fillna('n/a')
    
    # Convert start date to date type
    df['prd_start_dt'] = pd.to_datetime(df['prd_start_dt']).dt.date
    
    # Calculate end date using lead function
    df = df.sort_values(['prd_key', 'prd_start_dt'])
    df['prd_end_dt'] = df.groupby('prd_key')['prd_start_dt'].shift(-1)
    df['prd_end_dt'] = df['prd_end_dt'].apply(
        lambda x: (pd.to_datetime(x) - timedelta(days=1)).date() if pd.notna(x) else None
    )
    
    # Select and order columns
    columns = ['prd_id', 'cat_id', 'prd_key', 'prd_nm', 'prd_cost', 
               'prd_line', 'prd_start_dt', 'prd_end_dt']
    
    return df[columns].reset_index(drop=True)

def transform_sales_details(df):
    """Transform Sales Details table"""
    df = df.copy()
    
    # Convert date columns (handle 0 and invalid lengths)
    def convert_date(date_val):
        if pd.isna(date_val) or date_val == 0 or len(str(int(date_val))) != 8:
            return None
        try:
            return pd.to_datetime(str(int(date_val)), format='%Y%m%d').date()
        except:
            return None
    
    df['sls_order_dt'] = df['sls_order_dt'].apply(convert_date)
    df['sls_ship_dt'] = df['sls_ship_dt'].apply(convert_date)
    df['sls_due_dt'] = df['sls_due_dt'].apply(convert_date)
    
    # Calculate correct sales amount
    calculated_sales = df['sls_quantity'] * df['sls_price'].abs()
    df['sls_sales'] = np.where(
        (df['sls_sales'].isna()) | (df['sls_sales'] <= 0) | (df['sls_sales'] != calculated_sales),
        calculated_sales,
        df['sls_sales']
    )
    
    # Calculate correct price
    df['sls_price'] = np.where(
        (df['sls_price'].isna()) | (df['sls_price'] <= 0),
        df['sls_sales'] / df['sls_quantity'].replace(0, np.nan),
        df['sls_price']
    )
    
    # Select and order columns
    columns = ['sls_ord_num', 'sls_prd_key', 'sls_cust_id', 'sls_order_dt', 
               'sls_ship_dt', 'sls_due_dt', 'sls_sales', 'sls_quantity', 'sls_price']
    
    return df[columns].reset_index(drop=True)

def transform_cust_az12(df):
    """Transform Customer AZ12 table"""
    df = df.copy()
    
    # Remove 'NAS' prefix from customer ID
    df['cid'] = df['cid'].apply(
        lambda x: x[3:] if isinstance(x, str) and x.startswith('NAS') else x
    )
    
    # Set future birthdates to None
    today = datetime.now().date()
    df['bdate'] = pd.to_datetime(df['bdate'], errors='coerce')
    df['bdate'] = df['bdate'].apply(
        lambda x: None if pd.notna(x) and x.date() > today else x
    )
    
    # Normalize gender
    df['gen'] = df['gen'].str.upper().str.strip()
    gender_map = {
        'F': 'Female',
        'FEMALE': 'Female',
        'M': 'Male',
        'MALE': 'Male'
    }
    df['gen'] = df['gen'].map(gender_map).fillna('n/a')
    
    # Select and order columns
    columns = ['cid', 'bdate', 'gen']
    
    return df[columns].reset_index(drop=True)

def transform_loc_a101(df):
    """Transform Location A101 table"""
    df = df.copy()
    
    # Remove dashes from customer ID
    df['cid'] = df['cid'].str.replace('-', '')
    
    # Normalize country codes
    df['cntry'] = df['cntry'].str.strip()
    country_map = {
        'DE': 'Germany',
        'US': 'United States',
        'USA': 'United States'
    }
    df['cntry'] = df['cntry'].replace(country_map)
    df['cntry'] = df['cntry'].replace('', 'n/a').fillna('n/a')
    
    # Select and order columns
    columns = ['cid', 'cntry']
    
    return df[columns].reset_index(drop=True)

def transform_px_cat_g1v2(df):
    """Transform ERP PX Cat G1V2 table - no transformation needed"""
    columns = ['id', 'cat', 'subcat', 'maintenance']
    return df[columns].copy().reset_index(drop=True)

def main():
    """Main ETL execution function"""
    batch_start_time = datetime.now()
    
    try:
        print_header('Loading Silver Layer')
        
        print_subheader('Loading CRM Tables')
        
        # Transform cust_info
        start_time = datetime.now()
        print_step('Loading: bronze.cust_info')
        df_cust = load_bronze_data('cust_info.csv')
        print_step('Transforming: cust_info')
        df_cust_silver = transform_cust_info(df_cust)
        print_step('Saving to: silver.cust_info')
        save_silver_data(df_cust_silver, 'cust_info.csv')
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print_step(f'Load Duration: {duration:.2f} seconds')
        print_step('-------------')
        
        # Transform prd_info
        start_time = datetime.now()
        print_step('Loading: bronze.prd_info')
        df_prd = load_bronze_data('prd_info.csv')
        print_step('Transforming: prd_info')
        df_prd_silver = transform_prd_info(df_prd)
        print_step('Saving to: silver.prd_info')
        save_silver_data(df_prd_silver, 'prd_info.csv')
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print_step(f'Load Duration: {duration:.2f} seconds')
        print_step('-------------')
        
        # Transform sales_details
        start_time = datetime.now()
        print_step('Loading: bronze.sales_details')
        df_sales = load_bronze_data('sales_details.csv')
        print_step('Transforming: sales_details')
        df_sales_silver = transform_sales_details(df_sales)
        print_step('Saving to: silver.sales_details')
        save_silver_data(df_sales_silver, 'sales_details.csv')
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print_step(f'Load Duration: {duration:.2f} seconds')
        print_step('-------------')
        
        # Transform cust_az12
        start_time = datetime.now()
        print_step('Loading: bronze.cust_az12')
        df_erp_cust = load_bronze_data('cust_az12.csv')
        print_step('Transforming: cust_az12')
        df_erp_cust_silver = transform_cust_az12(df_erp_cust)
        print_step('Saving to: silver.cust_az12')
        save_silver_data(df_erp_cust_silver, 'cust_az12.csv')
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print_step(f'Load Duration: {duration:.2f} seconds')
        print_step('-------------')
        
        print_subheader('Loading ERP Tables')
        
        # Transform loc_a101
        start_time = datetime.now()
        print_step('Loading: bronze.loc_a101')
        df_loc = load_bronze_data('loc_a101.csv')
        print_step('Transforming: loc_a101')
        df_loc_silver = transform_loc_a101(df_loc)
        print_step('Saving to: silver.loc_a101')
        save_silver_data(df_loc_silver, 'loc_a101.csv')
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print_step(f'Load Duration: {duration:.2f} seconds')
        print_step('-------------')
        
        # Transform px_cat_g1v2
        start_time = datetime.now()
        print_step('Loading: bronze.px_cat_g1v2')
        df_cat = load_bronze_data('px_cat_g1v2.csv')
        print_step('Transforming: px_cat_g1v2')
        df_cat_silver = transform_px_cat_g1v2(df_cat)
        print_step('Saving to: silver.px_cat_g1v2')
        save_silver_data(df_cat_silver, 'px_cat_g1v2.csv')
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print_step(f'Load Duration: {duration:.2f} seconds')
        print_step('-------------')
        
        batch_end_time = datetime.now()
        total_duration = (batch_end_time - batch_start_time).total_seconds()
        
        print_header('Loading Silver Layer is Completed')
        print(f"   - Total Load Duration: {total_duration:.2f} seconds")
        print('=' * 80)
        
    except Exception as e:
        print_header('ERROR OCCURRED DURING LOADING SILVER LAYER')
        print(f'Error Message: {str(e)}')
        print('=' * 80)
        sys.exit(1)

if __name__ == '__main__':
    main()