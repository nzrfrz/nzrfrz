import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def main():
    # 1. Initialize Spark Session configured to talk to your local Hadoop HDFS
    # (If your HDFS runs on a different port than 9000, update it below)
    spark = SparkSession.builder \
        .appName("PySpark-WordCount-Benchmark") \
        .master("local[*]") \
        .getOrCreate()
    
    # We track time from the moment the actual data read/processing begins
    start_time = time.time()
    
    try:
        # 2. Read the text file from HDFS
        hdfs_path = "hdfs://localhost:9000/test-folder/input"
        text_df = spark.read.text(hdfs_path)
        
        # 3. Perform WordCount transformation
        words_df = text_df.select(explode(split(col("value"), r"\s+")).alias("word"))
        word_counts = words_df.filter(col("word") != "").groupBy("word").count()
        
        # 4. Write the results back to HDFS (Matches your MapReduce output directory)
        # Note: PySpark requires the output directory to NOT exist beforehand
        word_counts.write.mode("overwrite").csv("hdfs://localhost:9000/test-folder/output-spark")
        
        end_time = time.time()
        
        # 5. Calculate and print execution details
        elapsed_seconds = end_time - start_time
        print("\n" + "="*40)
        print("     PYSPARK BENCHMARK RESULTS      ")
        print("="*40)
        print(f"Total Seconds : {elapsed_seconds:.3f}")
        print(f"Total Milliseconds : {elapsed_seconds * 1000:.4f}")
        print("="*40 + "\n")
        
    finally:
        # Stop the spark session to free up JVM resources
        spark.stop()

if __name__ == "__main__":
    main()