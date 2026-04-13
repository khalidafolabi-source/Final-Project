# Final-Project

ShopNaija Daily Sales ETL Pipeline



1️ Project Title and Description

This project builds an automated ETL (Extract, Transform, Load) pipeline for ShopNaija, a Nigerian e-commerce platform. The pipeline extracts daily sales data from a CSV file, combines it with customer data from Supabase, converts prices from USD to Naira using a live API, cleans and validates the data, and loads the final dataset into a centralized database table for reporting.



2️ Data Sources

The pipeline uses three different data sources:

 1. Sales CSV File
	•	Contains daily transaction data
	•	Includes: order_id, product_name, category, quantity, unit_price_usd, order_date, customer_id, status
	•	File is dynamically selected using yesterday’s date (D_MINUS_1)



 2. Supabase Database (Customers Table)
	•	Stores customer information
	•	Includes: customer_id, name, city, loyalty_tier
	•	Data is fetched using the Supabase Python client



 3. Exchange Rate API
	•	URL: https://open.er-api.com/v6/latest/USD
	•	Provides real-time USD → NGN exchange rate
	•	Used to convert prices from USD to Naira



 How to Run

Follow these steps to run the project:

Step 1: Clone the repository
git clone <your-repo-link>
cd <your-project-folder>

2. Install required libraries
pip install pandas requests supabase

Step 3: Set environment variables (IMPORTANT)

Do NOT hardcode credentials in your code.
export SUPABASE_URL=your_url
export SUPABASE_KEY=your_key

tep 4: Run the pipeline

In Jupyter Notebook or Python:
un_pipeline()

Pipeline Flow

The pipeline follows this order:

Extract → Transform → Validate → Load
Flow Explanation:
	•	Extract: Read CSV, fetch customers, call API
	•	Transform: Clean data, merge datasets, calculate values
	•	Validate: Check data quality rules
	•	Load: Insert into Supabase table

  Data Cleaning Decisions

Several cleaning steps were applied to ensure data quality:
	•	Dropped duplicate rows
→ Prevents double-counting of sales
	•	Removed rows with missing unit_price_usd or customer_id
→ These are critical fields needed for calculations and joins
	•	Standardized column names (lowercase, no spaces)
→ Ensures consistency and prevents errors
	•	Removed cancelled orders
→ Cancelled transactions should not be included in revenue analysis
	•	Converted USD to NGN
→ Ensures business reporting is in local currency
	•	Handled missing values (NaN → None)
→ Required for successful database insertion

Cron Job Setup

The pipeline was automated using a cron job to run daily.

Steps:
	1.	Open terminal
2. Run
crontab -e
 0 7 * * * /usr/bin/python3 /path/to/your/script.py
 This runs the pipeline every day at 7 AM.

 Sample Output

After running the pipeline, the cleaned data is stored in Supabase table:

daily_sales_report

The table includes:
	•	Sales data
	•	Customer details
	•	Prices in USD and NGN
	•	Total transaction values





8️ Challenges and Learnings

 Challenges:
	•	Handling JSON errors during data insertion
	•	Fixing Timestamp and NaN issues
	•	Debugging column mismatches (e.g., quantity, status)
	•	Understanding how merging affects column names



 Learnings:
	•	Importance of data cleaning before loading
	•	How APIs integrate into ETL pipelines
	•	How to debug real-world data issues
	•	Best practices for structuring pipelines



 What I would improve next time:
	•	Add better logging system
	•	Automate file detection more robustly
	•	Add error alerting (email/notifications)
	•	Build a dashboard for visualization



 Conclusion

This project demonstrates the full lifecycle of a real-world data pipeline — from extracting raw data to delivering clean, structured insights into a database. It reflects practical data engineering skills used in modern applications.





Afolabi Khalid

Junior Data Engineer Project
:::
