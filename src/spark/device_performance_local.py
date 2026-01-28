from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round as spark_round, when
import time

print("=" * 80)
print("🔥 SPARK DEVICE PERFORMANCE ANALYSIS")
print("=" * 80)

start_time = time.time()

spark = SparkSession.builder \
    .appName("Device Performance - Spark") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session Created")

try:
    # Read data
    print("\n📥 Reading data...")
    df = spark.read.csv("data/generated/ad_events.csv", 
                        header=True, inferSchema=True)
    
    print(f"✅ Loaded {df.count():,} records")
    
    # Device Performance Analysis
    print("\n🔄 Analyzing device performance with Spark...")
    device_stats = df.groupBy("device_type").agg(
        count("*").alias("impressions"),
        sum("clicked").alias("clicks"),
        sum("converted").alias("conversions"),
        spark_round(sum("revenue"), 4).alias("total_revenue"),
        spark_round(sum("bid_price"), 4).alias("total_bid_value")
    )
    
    # Calculate metrics
    device_stats = device_stats.withColumn(
        "ctr_percent",
        spark_round((col("clicks") / col("impressions") * 100), 2)
    ).withColumn(
        "cvr_percent",
        when(col("clicks") > 0,
             spark_round((col("conversions") / col("clicks") * 100), 2)
        ).otherwise(0.0)
    ).withColumn(
        "avg_bid_price",
        spark_round((col("total_bid_value") / col("impressions")), 4)
    ).withColumn(
        "avg_revenue",
        spark_round((col("total_revenue") / col("impressions")), 4)
    ).withColumn(
        "revenue_per_click",
        when(col("clicks") > 0,
             spark_round((col("total_revenue") / col("clicks")), 4)
        ).otherwise(0.0)
    )
    
    device_stats = device_stats.orderBy(col("total_revenue").desc())
    
    # Show results
    print("\n" + "=" * 80)
    print("📱 DEVICE PERFORMANCE METRICS (SPARK PROCESSING)")
    print("=" * 80)
    device_stats.show(truncate=False)
    
    # Save results
    pandas_df = device_stats.toPandas()
    pandas_df.to_csv("data/processed/spark_device_analysis.csv", index=False)
    
    end_time = time.time()
    print(f"\n⚡ Spark Execution Time: {end_time - start_time:.2f} seconds")
    print("\n✅ DEVICE PERFORMANCE ANALYSIS COMPLETED!")
    
    spark.stop()

except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()