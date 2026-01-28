-- Create database
CREATE DATABASE IF NOT EXISTS pubmatic_analytics;
USE pubmatic_analytics;

-- Ad Events Table (main table)
CREATE EXTERNAL TABLE IF NOT EXISTS ad_events (
    timestamp STRING,
    publisher_id STRING,
    advertiser_id STRING,
    campaign_id STRING,
    device_type STRING,
    user_id STRING,
    region STRING,
    clicked INT,
    converted INT,
    bid_price DOUBLE,
    revenue DOUBLE,
    is_suspicious INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/pubmatic/input/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Publisher Revenue Summary (from MapReduce output)
CREATE EXTERNAL TABLE IF NOT EXISTS publisher_revenue (
    publisher_id STRING,
    impressions BIGINT,
    clicks BIGINT,
    conversions BIGINT,
    total_revenue DOUBLE,
    ctr_percent DOUBLE,
    cvr_percent DOUBLE,
    avg_revenue DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/pubmatic/output/revenue/'
TBLPROPERTIES ("skip.header.line.count"="0");

-- Device Performance (from MapReduce output)
CREATE EXTERNAL TABLE IF NOT EXISTS device_performance (
    device_type STRING,
    impressions BIGINT,
    clicks BIGINT,
    conversions BIGINT,
    total_revenue DOUBLE,
    ctr_percent DOUBLE,
    cvr_percent DOUBLE,
    avg_bid_price DOUBLE,
    avg_revenue DOUBLE,
    revenue_per_click DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/pubmatic/output/device/'
TBLPROPERTIES ("skip.header.line.count"="0");

-- Campaign ROI (from MapReduce output)
CREATE EXTERNAL TABLE IF NOT EXISTS campaign_roi (
    campaign_id STRING,
    advertiser_id STRING,
    impressions BIGINT,
    clicks BIGINT,
    conversions BIGINT,
    total_spend DOUBLE,
    total_revenue DOUBLE,
    ctr_percent DOUBLE,
    cvr_percent DOUBLE,
    cost_per_impression DOUBLE,
    cost_per_click DOUBLE,
    cost_per_conversion DOUBLE,
    roi_percent DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/pubmatic/output/campaign/'
TBLPROPERTIES ("skip.header.line.count"="0");

-- Fraud Detection (from MapReduce output)
CREATE EXTERNAL TABLE IF NOT EXISTS fraud_detection (
    user_id STRING,
    impressions BIGINT,
    clicks BIGINT,
    conversions BIGINT,
    publishers_count INT,
    click_rate DOUBLE,
    fraud_reason STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/pubmatic/output/fraud/'
TBLPROPERTIES ("skip.header.line.count"="0");