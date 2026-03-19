from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from config.config import Config

def main(spark: SparkSession):
    input_path = f"{Config.DATA_DIR}/stage/macro.parquet"
    output_path = f"{Config.DATA_DIR}/curated/macro_avg.parquet"

    print("🔵 Reading staged macro data...")
    df = spark.read.parquet(input_path)

    print("🟡 Transforming macro indicators...")

    # Example: compute average value per indicator
    # Assumes columns: indicator, value, date
    agg = (
        df.groupBy("indicator")
          .agg(avg(col("value")).alias("avg_value"))
    )

    print("🟢 Writing curated output...")
    agg.write.mode("overwrite").parquet(output_path)

    print("✨ Macro transformation complete!")