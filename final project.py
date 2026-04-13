#!/usr/bin/env python
# coding: utf-8

# In[15]:


import pandas as pd
import logging
import os
import psycopg2
import json
import requests
from supabase import create_client
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv(".env")

DB_HOST = os.getenv("aws-1-eu-central-1.pooler.supabase.com")
DB_PORT = os.getenv("5432")
DB_NAME = os.getenv("postgres")
DB_USER = os.getenv("postgres.yrrqxyumwzbocuijewsl")
DB_PASSWORD = os.getenv("Khalidtechky")

SUPABASE_URL = os.getenv("https://yrrqxyumwzbocuijewsl.supabase.co")
SUPABASE_KEY = os.getenv("sb_publishable_Su1KCmUaQI9iqRukOxkpgw_kYPmn1Ow")


# In[16]:


df = pd.read_csv("sales_2026-04-06.csv")


# In[17]:


df.head()


# In[18]:


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Project started successfully")


# In[19]:


def extract_sales(filepath):
    logging.info(f"Extracting sales data from {filepath}")

    try:
        df = pd.read_csv(filepath)
        logging.info(f"Sales data extracted successfully. Shape: {df.shape}")
        print(df.head())  # REQUIRED for testing step-by-step
        return df

    except Exception as e:
        logging.error(f"Failed to read CSV file: {e}")
        return None


# In[20]:


sales_df = extract_sales("sales_2026-04-06.csv")


# In[21]:


def transform_sales(df):
    logging.info("Starting sales data transformation")

    try:
        # Convert date
        df["order_date"] = pd.to_datetime(df["order_date"], dayfirst=True)
        logging.info("Converted order_date to datetime")

        #  Fix numeric columns
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["unit_price_usd"] = pd.to_numeric(df["unit_price_usd"], errors="coerce")

        logging.info("Converted quantity and unit_price_usd to numeric")

        #  Create total value in USD
        df["total_value_usd"] = df["quantity"] * df["unit_price_usd"]
        logging.info("Created total_value_usd column")

        #  Check missing values
        missing = df.isnull().sum()
        logging.info(f"Missing values per column:\n{missing}")

        print(df.head())

        logging.info("Sales transformation completed successfully")

        return df

    except Exception as e:
        logging.error(f"Sales transformation failed: {e}")
        return None


# In[22]:


sales_transformed = transform_sales(sales_df)


# In[23]:


supabase = create_client("https://yrrqxyumwzbocuijewsl.supabase.co", "sb_publishable_Su1KCmUaQI9iqRukOxkpgw_kYPmn1Ow")

logging.info("Supabase client created successfully")


# In[24]:


def extract_customers():
    logging.info("Extracting customers data from Supabase")

    try:
        response = supabase.table("customers").select("*").execute()

        df_customers = pd.DataFrame(response.data)

        logging.info(f"Customers extracted successfully. Shape: {df_customers.shape}")

        print(df_customers.head())

        return df_customers

    except Exception as e:
        logging.error(f"Failed to extract customers: {e}")
        return None


# In[25]:


customers_df = extract_customers()


# In[26]:


sales_df["order_date"] = pd.to_datetime(sales_df["order_date"], dayfirst=True)
sales_df["total_value_usd"] = sales_df["quantity"] * sales_df["unit_price_usd"]


# In[46]:


customers_df.head()
sales_df.head()


# In[47]:


def merge_data(sales_df, customers_df):
    logging.info("Merging datasets")

    merged_df = pd.merge(
        sales_df,
        customers_df,
        on="customer_id",
        how="left"
    )

    logging.info(f"Merged data shape: (merged_df.shape)")
    return merged_df


# In[48]:


df_merged = merge_data(sales_df, customers_df)
df_merged.head()


# In[49]:


df_merged["total_value_usd"] = df_merged["quantity_x"] * df_merged["unit_price_usd_x"]


# In[50]:


exchange_rate = 1500  # example (replace with API later)

df_merged["total_value_ngn"] = df_merged["total_value_usd"] * exchange_rate


# In[51]:


df_final = df_merged.dropna()   #clean final dataset


# In[31]:


pd.read_csv("sales_2026-04-06.csv")


# In[32]:


from datetime import datetime, timedelta

today = datetime.today()
D_MINUS_1 = today - timedelta(days=1)

date_str = D_MINUS_1.strftime('%Y-%m-%d')

date_str


# In[33]:


filename = f"sales_{"sales_2026-04-06"}.csv"

filename


# In[34]:


def extract_sales(filename):
    logging.info(f"Reading file: {filename}")

    try:
        df = pd.read_csv(filename)
        logging.info(f"Sales data loaded: {df.shape}")
        return df

    except Exception as e:
        logging.error(f"Failed to load sales file: {e}")
        return None



# In[35]:


sales_df = extract_sales("sales_2026-04-06.csv")
sales_df.head()


# In[36]:


def extract_customers():
    logging.info("Extracting customers from Supabase")

    try:
        response = supabase.table("customers").select("*").execute()
        df = pd.DataFrame(response.data)

        logging.info(f"Customers loaded: {df.shape}")
        return df

    except Exception as e:
        logging.error(f"Failed to extract customers: {e}")
        return None


# In[37]:


customers_df = extract_customers()
customers_df.head()


# In[38]:


url = "https://open.er-api.com/v6/latest/USD"

response = requests.get(url)
data = response.json()

ngn_rate = data["rates"]["NGN"]

print(ngn_rate)


# In[39]:


def extract_exchange_rate():
    logging.info("Fetching live exchange rate from API")

    try:
        url = "https://open.er-api.com/v6/latest/USD"
        response = requests.get(url)
        data = response.json()

        rate = data["rates"]["NGN"]

        logging.info(f"Exchange rate fetched: 1 USD = {rate} NGN")

        return rate

    except Exception as e:
        logging.error(f"Failed to fetch exchange rate: {e}")
        return None


# In[52]:


df_merged["total_value_ngn"] = df_merged["total_value_usd"] * exchange_rate


# In[53]:


exchange_rate = extract_exchange_rate()
exchange_rate


# In[54]:


df_merged["total_value_ngn"] = df_merged["total_value_usd"] * exchange_rate


# In[60]:


sales_df = sales_df.drop_duplicates()   #drop duplicates

sales_df = sales_df.dropna(subset=["unit_price_usd"])    #remove missing unit price usd

sales_df = sales_df.dropna(subset=["customer_id"])    #remove missing customer id

sales_df.columns = sales_df.columns.str.strip().str.lower()
customers_df.columns = customers_df.columns.str.strip().str.lower()     #clean column names

df_merged = df_merged[df_merged["status_x"].str.lower() != "cancelled"]     #remove cancelled orders

df_merged["unit_price_ngn"] = df_merged["unit_price_usd_x"] * exchange_rate    #currency conversion

df_merged["total_value_ngn"] = df_merged["unit_price_ngn"] * df_merged["quantity_x"]    #total value ngn column


# In[62]:


def validate_data(df):
    print(" Running data validation checks...")

    #  Check nulls
    if df.isnull().sum().sum() > 0:                  #ensures no missing values
        print(" WARNING: There are still null values in the dataset")
    else:
        print(" No null values found")

    #  quantity > 0
    if (df["quantity"] <= 0).any():     #ensures quantity is real with no 0 or negative sales
        print(" WARNING: Some quantity values are <= 0")
    else:
        print(" All quantity values are valid")

    #  unit_price_usd > 0          #ensures pricing is valid
    if (df["unit_price_usd"] <= 0).any():
        print(" WARNING: Some unit_price_usd values are <= 0")
    else:
        print(" All unit_price_usd values are valid")

    #  rows count: shows final row counting 
    print(f" Rows ready for loading: {len(df)}")


# In[65]:


df_final = df_merged  #ensure dataframe is ready


# In[66]:


df_final = df_final.where(pd.notnull(df_final), None)    #clean


# In[137]:


def load_to_supabase(df, exchange_rate):
    import psycopg2
    try:
        logging.info("Connecting to Supabase..")

        conn = psycopg2.connect(
            host="aws-1-eu-central-1.pooler.supabase.com",
            database="postgres",  # Changed - to =
            user="postgres.yrrqxyumwzbocuijewsl", # Ensure this has =
            password="Khalidtechky", # Ensure this has =
            port=5432
        )

        cursor = conn.cursor() 

        insert_query = """
        INSERT INTO daily_sales_report (
            customer_id,
            quantity,
            unit_price_usd,
            total amount_usd,
            total_onount_ngn,
            order date
        )VALUES（（%，%，%，%，%，%）
        """


        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                row["customer_id"],
                row["quantity"],
                row["unit_price_usd"],
                row["total_amount_usd"],
                row("total_amount_ngn"),
                row["order_date"]
            ))

            conn.commit()
            cursor.close()
            conn.close()


            print(f" Insert successful!")
            print(f" Rows inserted: {len(df_final)}")
            print(f" Exchange rate used: {exchange_rate}")

            logging.info("Data successfully loaded to supabase")

    except Exception as e:
        logging.error(f"Error loading to supabase: {e}")



# In[149]:


# main pipeline
def main():
    logging. info("Pipeline started")
    #Extract
    sales_df= extract_sales(filename)
    get_ipython().run_line_magic('pinfo', 'extract_customers')


    logging.info("Extraction step completed")
# Transform
    def transform_sales(sales_df, customers_df):
# Validate
        validate_data = validate_data(final_df)
#  Load
    load_to_supabase (validate_data, exchange_rate)
    logging.info("Pipeline finished successfully")


# In[150]:


main()


# In[ ]:




