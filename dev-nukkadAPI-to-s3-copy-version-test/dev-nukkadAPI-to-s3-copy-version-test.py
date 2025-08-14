import sys
import json
import requests
from datetime import datetime, timedelta
import time

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, col
from pyspark.sql import Row
from awsglue.dynamicframe import DynamicFrame
import pytz


# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("dev-nukkadAPI-to-s3-history", {})

# Static values
ndcin_list = [
    "NDCIN1782", "NDCIN1851", "NDCIN1799", "NDCIN1800",
    "NDCIN2045", "NDCIN2003", "NDCIN1990", "NDCIN1991",
    "NDCIN1992", "NDCIN1993", "NDCIN2002", "NDCIN1995",
    "NDCIN1997", "NDCIN1635"
]

API_BASE_URL = 'https://integrations.nukkadshops.com/dino/v1/'
ENDPOINT = 'sales/getSalesWithItems'
API_URL = API_BASE_URL + ENDPOINT

headers = {
    'X-Nukkad-API-Token': 'wYVgXJfi5ltbTvx4kwG23FeNMWHN7ruV',
    'Content-Type': 'application/json'
}

logs_list = []

# Loop till yesterday
ist = pytz.timezone('Asia/Kolkata')
start_date = datetime.now(ist) - timedelta(days=1)
end_date = datetime.now(ist) - timedelta(days=1)
date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

for current_date in date_range:
    try:
        today_str = current_date.strftime('%Y-%m-%d')
        year, month, day = today_str.split('-')
        output_path = f"s3://hoi-ganit-files/api-parquet-data/nukkad-api-data/{year}/{month}/{day}/"

        # Create start and end of day in IST, convert to UTC for timestamp
        start_ist = ist.localize(datetime(current_date.year, current_date.month, current_date.day, 0, 0, 0))
        end_ist = ist.localize(datetime(current_date.year, current_date.month, current_date.day, 23, 59, 59))

        # Convert IST to UTC and get epoch seconds
        from_ts = int(start_ist.astimezone(pytz.utc).timestamp())
        to_ts = int(end_ist.astimezone(pytz.utc).timestamp())

        print(f"\n📅 Processing date: {today_str} --> {from_ts} to {to_ts} (UTC epoch)")

        # Capture start time of API call
        start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Initial payload
        base_payload = {
            "from": str(from_ts),
            "to": str(to_ts),
            "pageNo": "1"
        }

        # Initial request
        response = requests.post(API_URL, headers=headers, json=base_payload)
        if response.status_code != 200:
            raise Exception(f"API call failed: {response.status_code} - {response.text}")
        
        json_response = response.json()
        try:
            page_count = json_response['data']['pageCount']
            bills = json_response['data']['bills']
            if not bills:
                raise Exception("Bills list is empty on page 1")
        except Exception as e:
            raise Exception(f"Failed to extract initial data: {str(e)}")

        df_all = spark.read.json(spark.sparkContext.parallelize(bills))

        for page in range(2, page_count + 1):
            payload = {
                "from": str(from_ts),
                "to": str(to_ts),
                "pageNo": str(page)
            }

            print(f"Fetching page {page} of {page_count} for {today_str}")
            response = requests.post(API_URL, headers=headers, json=payload)
            if response.status_code != 200:
                print(f"Failed to fetch page {page}: {response.status_code}")
                continue
            try:
                page_bills = response.json()['data']['bills']
                if page_bills:
                    df_page = spark.read.json(spark.sparkContext.parallelize(page_bills))
                    df_all = df_all.unionByName(df_page, allowMissingColumns=True)
            except Exception as e:
                print(f"Error processing page {page}: {str(e)}")

        # Add date column
        df_all = df_all.withColumn("ingest_date", lit(today_str))
        df_filtered = df_all.filter(col("ndcin").isin(ndcin_list))

        df_filtered.write.mode("overwrite").parquet(output_path)

        row_count = df_filtered.count()
        distinct_ndcin = df_filtered.select("ndcin").distinct().orderBy("ndcin").rdd.flatMap(lambda x: x).collect()

        print(f"✅ {row_count} rows written to {output_path} for NDCINs: {distinct_ndcin}")

        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logs_list.append({
            "api": "Nukkad",
            "extraction_date": today_str,
            "start_time": start_time,
            "end_time": end_time,
            "row_count": row_count,
            "page_count": page_count,
            "type": "Ingestion",
            "error": "No Error",
            "is_failed": False
        })

    except Exception as e:
        error_message = str(e)
        end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"❌ Error on {current_date.strftime('%Y-%m-%d')}: {error_message}")
        logs_list.append({
            "api": "Nukkad",
            "extraction_date": current_date.strftime('%Y-%m-%d'),
            "start_time": start_time,
            "end_time": end_time,
            "row_count": 0,
            "page_count": 0,
            "type": "Ingestion",
            "error": error_message,
            "is_failed": True
        })

# ---- Write audit logs to Redshift ----
logs_df = spark.createDataFrame([Row(**log) for log in logs_list])
logs_df.show(truncate=False)

dyf = DynamicFrame.fromDF(logs_df, glueContext, "dyf")
glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="redshift",
    connection_options={
        "url": "jdbc:redshift://pax-anlytics-dev-redshift-serverless-wg.467988629675.ap-south-1.redshift-serverless.amazonaws.com:5439/projectdb",
        "user": "admin",
        "password": "5sBLOJdxkdyq9k1",
        "redshiftTmpDir": 's3://hoi-ganit-files/redshift/',
        "dbtable": "public.api_audit_logs",
    }
)

job.commit()
