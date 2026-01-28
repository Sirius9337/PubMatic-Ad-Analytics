from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, round as spark_round, when
import time

print("=" * 80)
print("🔥 SPARK REVENUE ANALYSIS - PubMatic Analytics")
print("=" * 80)

start_time = time.time()

# Initialize Spark in local mode
spark = SparkSession.builder \
    .appName("PubMatic Revenue Analysis - Spark") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark Session Created (Local Mode)")
print(f"📍 Spark Version: {spark.version}")

try:
    # Read from local CSV
    print("\n📥 Reading data from CSV...")
    df = spark.read.csv("data/generated/ad_events.csv", 
                        header=True, inferSchema=True)
    
    total_records = df.count()
    print(f"✅ Loaded {total_records:,} records")
    
    # Show sample
    print("\n📋 Sample Data:")
    df.show(5, truncate=False)
    
    # Publisher Revenue Analysis
    print("\n🔄 Processing publisher revenue metrics with Spark...")
    publisher_stats = df.groupBy("publisher_id").agg(
        count("*").alias("impressions"),
        sum("clicked").alias("clicks"),
        sum("converted").alias("conversions"),
        spark_round(sum("revenue"), 4).alias("total_revenue")
    )
    
    # Calculate metrics
    publisher_stats = publisher_stats.withColumn(
        "ctr_percent", 
        spark_round((col("clicks") / col("impressions") * 100), 2)
    ).withColumn(
        "cvr_percent",
        when(col("clicks") > 0, 
             spark_round((col("conversions") / col("clicks") * 100), 2)
        ).otherwise(0.0)
    ).withColumn(
        "avg_revenue",
        spark_round((col("total_revenue") / col("impressions")), 4)
    )
    
    # Sort by revenue
    publisher_stats = publisher_stats.orderBy(col("total_revenue").desc())
    
    # Show results
    print("\n" + "=" * 80)
    print("🏆 TOP 20 PUBLISHERS BY REVENUE (SPARK PROCESSING)")
    print("=" * 80)
    publisher_stats.show(20, truncate=False)
    
    # Summary statistics
    print("\n📊 SPARK SUMMARY STATISTICS:")
    print(f"Total Publishers: {publisher_stats.count()}")
    print(f"Total Impressions: {df.count():,}")
    print(f"Total Clicks: {df.agg(sum('clicked')).collect()[0][0]:,}")
    print(f"Total Conversions: {df.agg(sum('converted')).collect()[0][0]:,}")
    print(f"Total Revenue: ${df.agg(sum('revenue')).collect()[0][0]:,.2f}")
    
    # Save results
    output_dir = "data/processed/spark_results"
    print(f"\n💾 Saving results to: {output_dir}")
    
    publisher_stats.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        output_dir + "/revenue"
    )
    
    # Also save as single file for easy access
    pandas_df = publisher_stats.toPandas()
    pandas_df.to_csv("data/processed/spark_revenue_analysis.csv", index=False)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"\n⚡ Spark Execution Time: {execution_time:.2f} seconds")
    print("\n" + "=" * 80)
    print("✅ SPARK REVENUE ANALYSIS COMPLETED SUCCESSFULLY!")
    print("=" * 80)
    
    spark.stop()

except Exception as e:
    print(f"\n❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()