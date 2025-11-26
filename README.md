A simple demo of PySpark on Databricks. The assignment:

# PySpark Data Pipeline Project: E-Commerce Order Analytics

## Project Overview
Build an end-to-end data pipeline to analyze e-commerce order data using PySpark in Databricks. This project will help you practice data ingestion, transformation, aggregation, and storage operations.

## Dataset
You'll work with three CSV files representing an e-commerce system:

**customers.csv**
- customer_id (integer)
- customer_name (string)
- email (string)
- country (string)
- registration_date (date: YYYY-MM-DD)

**orders.csv**
- order_id (integer)
- customer_id (integer)
- order_date (date: YYYY-MM-DD)
- order_status (string: "Completed", "Cancelled", "Pending")
- total_amount (decimal)

**order_items.csv**
- item_id (integer)
- order_id (integer)
- product_name (string)
- category (string)
- quantity (integer)
- price_per_unit (decimal)

## Setup (Part 0)
1. Create sample CSV files with at least 100 customers, 500 orders, and 1000 order items
2. Upload these files to Databricks DBFS under `/FileStore/ecommerce/`
3. Create a new Databricks notebook for your solution

## Assignment Tasks

### Task 1: Data Ingestion (Load all three datasets)
Load each CSV file into a PySpark DataFrame with the following requirements:
- Infer schema automatically
- Include headers
- Display the first 5 rows of each dataset
- Print the schema of each dataset
- Show the row count for each dataset

**Expected outputs:** 3 DataFrames, 3 schema printouts, 3 row counts

---

### Task 2: Data Quality Checks
Perform the following checks and print results:
- Count null values in each column of each dataset
- Identify any duplicate order_ids in the orders dataset
- Check if there are any order_items referencing non-existent order_ids

**Expected outputs:** Null count report for each dataset, duplicate order count (should be 0), orphaned items count (should be 0)

---

### Task 3: Data Transformation
Create a new DataFrame called `orders_enriched` that:
- Joins orders with customers
- Includes columns: order_id, customer_id, customer_name, country, order_date, order_status, total_amount
- Filters to include only "Completed" orders
- Adds a new column `order_year` extracted from order_date
- Adds a new column `order_month` extracted from order_date

**Expected output:** A DataFrame with exactly 8 columns, showing only completed orders

---

### Task 4: Aggregation Analysis #1 - Revenue by Country
Create a DataFrame that shows:
- country
- total_revenue (sum of total_amount)
- order_count (number of orders)
- average_order_value (total_revenue / order_count)
- Sort by total_revenue in descending order

**Expected output:** One row per country with 4 columns, sorted by revenue

---

### Task 5: Aggregation Analysis #2 - Monthly Trends
Create a DataFrame that shows:
- order_year
- order_month
- monthly_revenue (sum of total_amount)
- monthly_order_count (number of orders)
- Sort by year and month in ascending order

**Expected output:** One row per year-month combination with 4 columns, chronologically sorted

---

### Task 6: Product Category Analysis
Create a DataFrame that:
- Joins order_items with orders (only completed orders)
- Groups by category
- Shows: category, total_quantity_sold, total_category_revenue, number_of_orders
- Sort by total_category_revenue in descending order

**Expected output:** One row per category with 4 columns

---

### Task 7: Top Customers
Create a DataFrame that identifies the top 10 customers by total spending:
- customer_id
- customer_name
- total_spent (sum of all their completed orders)
- order_count
- average_order_value
- Limit to exactly 10 rows

**Expected output:** Exactly 10 rows showing top spenders with 5 columns

---

### Task 8: Data Quality - Handle Edge Cases
Create a cleaned version of the orders dataset called `orders_clean` that:
- Replaces any null values in total_amount with 0
- Removes any orders where order_date is null
- Adds a new column `is_high_value` which is True if total_amount > 200, otherwise False
- Show the count of high-value vs regular orders

**Expected output:** Cleaned DataFrame and a count showing distribution of high_value flag

---

### Task 9: Write Results to Storage
Save the following DataFrames to DBFS in parquet format:
- `orders_enriched` → `/FileStore/ecommerce/output/orders_enriched/`
- Revenue by country → `/FileStore/ecommerce/output/revenue_by_country/`
- Top 10 customers → `/FileStore/ecommerce/output/top_customers/`

Write in overwrite mode with a single partition.

**Expected output:** 3 successful write operations, verify files exist in DBFS

---

### Task 10: Read and Verify
Read back one of the saved parquet files (revenue_by_country) and:
- Display the schema
- Show all rows
- Confirm the data matches what you originally calculated

**Expected output:** Schema printout and full data display matching Task 4 results

---

## Evaluation Criteria
Your solution is correct if:
- All DataFrames contain the exact columns specified
- Row counts are logical (e.g., filtering reduces rows)
- Aggregations produce one row per group as specified
- Sort orders match requirements
- All outputs are displayed in your notebook
- Files are successfully written to and read from DBFS

## Bonus Challenge (Optional)
Create a single comprehensive report DataFrame that joins all your analyses and includes:
- Total number of customers
- Total number of completed orders
- Overall revenue
- Top performing country
- Top performing category
- Date range of the data

This should produce a single-row summary DataFrame.
