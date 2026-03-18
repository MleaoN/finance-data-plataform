from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

def main():
    spark = (
        SparkSession.builder
        .appName("market_transform")
        .getOrCreate()
    )

    print("🔵 Reading staged market data...")
    df = spark.read.parquet("/opt/app/data/stage/market.parquet")

    print("🟡 Transforming...")
    agg = (
        df.groupBy("ticker")
          .agg(avg(col("close")).alias("avg_close"))
    )

    print("🟢 Writing curated output...")
    agg.write.mode("overwrite").parquet("/opt/app/data/curated/market_avg.parquet")

    print("✨ Done!")
    spark.stop()

if __name__ == "__main__":
    main()