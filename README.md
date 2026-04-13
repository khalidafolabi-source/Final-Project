# Final project



#### ShopNaija Daily Data Pipeline
 ###### Project Overview

ShopNaija is a fast-growing Nigerian e-commerce platform operating across Lagos, Abuja, and Port Harcourt. This project builds an automated data pipeline that consolidates daily sales, customer data, and live exchange rates into a centralized reporting table.

The pipeline performs data extraction, cleaning, transformation, validation, and loading (ETL) — and runs automatically every day at 7:00 AM using a cron job.

 Data Sources
1. Sales Data (CSV Files)
Daily CSV file containing previous day’s sales

File format:

### sales_YYYY-MM-DD.csv
Automatically selected using a dynamic date variable (D_MINUS_1)
2. Customer Data (Supabase Database)
Stored in a Supabase PostgreSQL database
Table: customers
Contains:
Customer ID
Full Name
City
Loyalty Tier
3. Exchange Rate API

Endpoint:

https://open.er-api.com/v6/latest/USD
Provides real-time USD → NGN conversion rate
Used to convert all sales values to Naira
 How to Run the Project
1. Clone the Repository
git clone https://github.com/your-username/shopnaija-data-pipeline.git
cd shopnaija-data-pipeline
2. Install Dependencies
pip install pandas requests sqlalchemy psycopg2-binary python-dotenv
3. Set Up Environment Variables

Create a .env file:

DB_HOST=your_host

DB_PORT=5432

DB_NAME=postgres

DB_USER=your_user

DB_PASSWORD=your_password
4.#### Run the Pipeline
python main.py
 Pipeline Flow
Extract → Transform → Validate → Load

Steps Explained:

Extract data from CSV, Supabase, and API
Clean and transform the data
Validate data quality
Load into Supabase
 Data Cleaning Decisions
Removed duplicates
Ensures no repeated sales records
Dropped missing values
unit_price_usd → required for calculations
customer_id → required for merging
Standardized column names
Lowercase and no spaces for consistency
Filtered cancelled orders
Only valid transactions are included
Currency conversion
USD → NGN using live exchange rate
Derived columns added
unit_price_ngn
total_value_ngn
 Data Validation

The pipeline checks:

No null values in final dataset
All quantity values > 0
All unit_price_usd values > 0

 If any check fails:

A warning is printed
Pipeline continues running (no crash)
 Data Loading

Final dataset is loaded into Supabase table:

daily_sales_report
Output includes:
Customer details
Cleaned sales data
NGN values
 Cron Job Automation

The pipeline runs automatically every day at 7:00 AM.

Example Cron Job:
0 7 * * * /usr/bin/python3 /path/to/your/project/main.py
Explanation:
0 7 * * * → Runs daily at 7:00 AM
Executes the pipeline script automatically

 

 Sample Output

After execution, the daily_sales_report table contains:

Cleaned and merged dataset
NGN-converted prices
Valid transactions only



 Challenges & Learnings
Challenges
Handling dynamic file naming using dates
Connecting securely to Supabase
Managing API requests reliably
Ensuring clean merges between datasets
Learnings
Importance of modular pipeline design (functions)
Real-world data is messy — cleaning is critical
Automation (cron jobs) is essential in production
Validation ensures trust in data
# Project Structure
 
 
 shopnaija-data-pipeline/
│


├── main.py

├── pipeline/

│   ├── extract.py

│   ├── transform.py

│   ├── validate.py

│   └── load.py

│

├── data/

│   └── sales_YYYY-MM-DD.csv

│

├── .env

├── requirements.txt

└── README.md
### Future Improvements
Add logging system instead of print statements
Implement retry logic for API failures
Add unit tests for each function
Deploy pipeline using Airflow or Prefect
