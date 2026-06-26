# Benchmark Analysis: Hadoop MapReduce vs. Apache Spark (DataFrame API)

This analysis compares the performance of a WordCount job executed on the same 100 MB HDFS text file (`examplefile-100mb.txt`), using **Hadoop MapReduce** and **Apache Spark's DataFrame API**, on a local single-node cluster (`local[8]`, Windows 11).

## 1. Benchmark Results Table

| Testing Component | Hadoop MapReduce | Apache Spark (DataFrame API) |
| :--- | :--- | :--- |
| **File Input Size** | ~100 MB (`examplefile-100mb.txt`, 104,857,621 bytes read) | ~100 MB (same file, read via `hdfs://localhost:9000/test-folder/input/examplefile-100mb.txt`) |
| **Measurement Command** | `Measure-Command {hadoop jar "C:\Hadoop\hadoop-3.3.6\share\hadoop\mapreduce\hadoop-mapreduce-examples-3.3.6.jar" wordcount /test-folder/input /test-folder/output}` | `spark-submit --master local[8] --conf spark.pyspark.python=<venv>\Scripts\python.exe benchmark-spark.py` (internal `time.time()` instrumentation around read → transform → write) |
| **Total Executed Time** | **26.790 seconds** (26,790.44 ms) | **8.328 seconds** (8,328.05 ms) |
| **Success Status** | ✅ Success — `Job job_1782457030367_0002 completed successfully` | ✅ Success — job completed, `SparkContext` stopped with exit code 0 |

> Spark completed the same WordCount workload roughly **3.2x faster** than Hadoop MapReduce (8.328s vs 26.790s).

## 2. Which framework is faster, and why?

**Apache Spark is faster** (8.328s vs. 26.790s — about 3.2x speedup on this 100 MB dataset).

The difference comes down to how each engine handles intermediate data between processing stages:

- **Hadoop MapReduce is disk-based.** After the Map phase, intermediate key-value pairs are sorted, spilled to local disk, and shuffled to Reducers, which read that data back from disk before producing final output. Every map task's output and every spill is persisted to disk, and the job log confirms this overhead (`Spilled Records=35`, `Total time spent by all maps in occupied slots (ms)=8799`, plus separate map (100%) and reduce (100%) phases that run sequentially). Disk I/O latency dominates the runtime, especially the read → write → read → write cycle between Map and Reduce.

- **Spark is in-memory.** Spark's DataFrame API builds a DAG of transformations (`read.text` → `explode/split` → `filter` → `groupBy/count` → `write.csv`) and executes it across in-memory RDD/DataFrame partitions. Data shuffled between stages (e.g., the `groupBy("word")` aggregation) is kept in memory (`MemoryStore started with capacity 2004.6 MiB`) whenever possible, only spilling to disk under memory pressure. Because intermediate results don't have to round-trip through disk the way MapReduce's Map→Reduce handoff does, Spark avoids the bulk of the I/O latency that dominates the Hadoop run.

In short: Hadoop MapReduce pays a disk read/write cost at every stage boundary, while Spark keeps intermediate data in memory across the DAG of transformations, which is the core reason its execution time is significantly lower for the same workload.

## 3. Why does Hadoop MapReduce show a delay before progress moves past "map 0% reduce 0%"? What is YARN ResourceManager's role?

Looking at the terminal log timestamps:

```
14:23:29,887 INFO mapreduce.Job: Running job: job_1782457030367_0002
14:23:36,975 INFO mapreduce.Job: Job ... running in uber mode : false
14:23:36,976 INFO mapreduce.Job:  map 0% reduce 0%
14:23:48,058 INFO mapreduce.Job:  map 100% reduce 0%
```

There's a visible gap (~7 seconds before "map 0% reduce 0%" even appears, then ~11 more seconds before any map progress is reported) where nothing appears to be happening from the client's point of view. This delay is the **resource negotiation and container bootstrap phase**, and it is driven by the **YARN ResourceManager (RM)**:

1. **Application submission** — The client (`hadoop jar ...`) connects to the ResourceManager (`Connecting to ResourceManager at /0.0.0.0:8032`) and submits the job as a new YARN Application (`Submitted application application_1782457030367_0002`).
2. **ApplicationMaster (AM) allocation** — The ResourceManager's Scheduler must first find a NodeManager with enough free resources (memory/vCores) to launch the job's **ApplicationMaster container**. This is a negotiation step: the RM doesn't run the job itself, it only allocates the *first* container, in which the AM process will live.
3. **ApplicationMaster startup** — Once the AM container is allocated, the NodeManager has to launch the JVM for the ApplicationMaster, which then initializes and registers itself back with the ResourceManager. This JVM startup is part of the "silent" gap before any map/reduce progress is shown.
4. **Resource requests for Map/Reduce tasks** — Only after the AM is running does it request additional containers from the ResourceManager to actually run the Map and Reduce tasks. Each of those containers must again be scheduled, allocated, and launched by their respective NodeManagers before real computation starts.
5. **Heartbeat-driven progress reporting** — The client polls the AM periodically for status, which is why progress appears to "jump" (e.g., straight to `map 100%` once the single split's map task finishes) rather than updating continuously.

So the delay at the start is not the actual WordCount computation — it's the overhead of YARN's two-level resource negotiation (RM allocates a container for the AM → AM then requests/negotiates containers for the actual tasks), plus JVM startup time for each of those containers. Spark, in `local[8]` mode, skips this entirely since it runs in a single long-lived JVM process with no YARN container negotiation, which also contributes to its faster overall time.
