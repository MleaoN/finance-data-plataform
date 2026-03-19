import sys
from pyspark.sql import SparkSession

def run(job_name):
    spark = (
        SparkSession.builder
        .appName(f"spark_job_{job_name}")
        .getOrCreate()
    )

    if job_name == "market":
        from spark_jobs.transform_market import main
        main(spark)

    elif job_name == "macro":
        from spark_jobs.transform_macro import main
        main(spark)

    else:
        raise ValueError(f"❌ Unknown job: {job_name}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError("❌ Usage: spark-submit run_job.py <job_name>")

    job_name = sys.argv[1]
    run(job_name)