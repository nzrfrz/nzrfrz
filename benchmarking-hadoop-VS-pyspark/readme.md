```python
import os

readme_content = """# Big Data WordCount Benchmark (Hadoop MapReduce vs. PySpark)

This repository contains benchmarking guidelines and scripts to evaluate word count processing performance using **Hadoop MapReduce** and **Apache Spark (PySpark)** over an HDFS deployment.

---

## 📊 Benchmark Summary
* **Dataset Size:** ~100 MB text file (`examplefile-100mb.txt`)
* **Environment:** Local Single-Node Hadoop cluster running on Windows 11
* **Hardware Resources Allocated:** `local[8]` (8 CPU threads / Cores)

### ⏱️ Performance Results
| Framework | Execution Time | Performance Multiplier |
| :--- | :--- | :--- |
| **Hadoop MapReduce** | 26.790 seconds | 1.0x (Baseline) |
| **Apache PySpark** | **8.328 seconds** | **3.2x Faster** |

---

## ⚙️ Prerequisites & Setup

Ensure the following system environment variables are configured on your cluster:
* `JAVA_HOME` pointing to JDK 1.8+
* `HADOOP_HOME` pointing to your local Hadoop installation (e.g., v3.3.6)
* `SPARK_HOME` pointing to your local Apache Spark binary (e.g., v3.5.1)

---

## 🚀 Execution Instructions

### 1. Data Preparation (HDFS)
Ensure your input directory exists in HDFS, upload your text data file, and ensure old output paths are cleared:


```

```text
File written successfully.

```bash
# Create input directory in HDFS
hadoop fs -mkdir -p /YOUR_INPUT_FOLDER

# Upload your target text document
hadoop fs -put /PATH/TO/YOUR/LOCAL/DATASET/examplefile-100mb.txt /YOUR_INPUT_FOLDER/

# Clean up old output paths before running benchmarks
hadoop fs -rm -r /YOUR_OUTPUT_FOLDER_MR
hadoop fs -rm -r /YOUR_OUTPUT_FOLDER_SPARK

```

### 2. Running the Hadoop MapReduce Benchmark

To capture full pipeline execution metrics on Windows, wrap the command using PowerShell's `Measure-Command` tool:

```powershell
Measure-Command {hadoop jar "/PATH/TO/HADOOP/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar" wordcount /YOUR_INPUT_FOLDER /YOUR_OUTPUT_FOLDER_MR}

```

### 3. Running the PySpark Benchmark

First, configure your PySpark benchmarking script (`benchmark-spark.py`) with your HDFS endpoints:

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def main():
    # Initialize optimized Spark Session
    spark = SparkSession.builder \\
        .appName("PySpark-WordCount-Benchmark") \\
        .master("local[8]") \\
        .getOrCreate()
    
    start_time = time.time()
    
    try:
        # Load file directly from your cluster's NameNode HDFS path
        hdfs_input_path = "hdfs://localhost:9000/YOUR_INPUT_FOLDER"
        text_df = spark.read.text(hdfs_input_path)
        
        # In-memory MapReduce transformations
        words_df = text_df.select(explode(split(col("value"), r"\\s+")).alias("word"))
        word_counts = words_df.filter(col("word") != "").groupBy("word").count()
        
        # Write partitions back to HDFS
        hdfs_output_path = "hdfs://localhost:9000/YOUR_OUTPUT_FOLDER_SPARK"
        word_counts.write.mode("overwrite").csv(hdfs_output_path)
        
        end_time = time.time()
        elapsed_seconds = end_time - start_time
        
        print("\\n" + "="*40)
        print(f"Total Seconds : {elapsed_seconds:.3f}")
        print("="*40 + "\\n")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

```

Navigate to your script workspace folder and submit the job utilizing your explicit Python virtual environment interpreter configs:

```bash
cd /PATH/TO/YOUR/WORKFLOW_WORKSPACE

spark-submit --master local[8] --conf "spark.pyspark.python=/PATH/TO/YOUR/VENV/Scripts/python.exe" benchmark-spark.py

```
