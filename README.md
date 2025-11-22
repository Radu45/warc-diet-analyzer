# WARC Keto vs Vegan Popularity Analyzer
This repository contains a Scala application built with Apache Spark that analyzes WARC (Web ARChive) files to identify web pages mentioning two major diet trends—keto and 
vegan—and compare their popularity across the web.

## Project Overview

1. The application processes large-scale web data to:
2. Identify pages that mention the keywords “keto” or “vegan”
3. Count and rank these pages based on the frequency of keyword mentions
4. Compare the popularity of both diets based on raw and standardized frequencies
5. Normalize results to account for differences in webpage length

## Requirements

- Apache Spark (2.12.x)
- Scala (2.12.x)
- SBT (Simple Build Tool)
- Jsoup (HTML Parser)
- Access to WARC files (Common Crawl)

## How to Build

Use the SBT assembly plugin to build a fat JAR with all dependencies:

```bash
sbt clean assembly
```
This will create a JAR file at:

```bash
target/scala-2.12/KetoVeganAnalyzer-assembly-1.0.jar
```
Note: Make sure to use sbt assembly rather than sbt package to properly include external dependencies like Jsoup.

## How to Run

The application can be run on a Spark cluster using the spark-submit command:

```bash
spark-submit --deploy-mode cluster --queue default target/scala-2.12/KetoVeganAnalyzer-assembly-1.0.jar

```

## Configuration

You can modify the following in the code:

1. Number of WARC files:
```bash
val warcFiles = (0 to 1).map(a => f"hdfs:/single-warc-segment/CC-MAIN-20210410105831-20210410135831-$a%05d.warc.gz")

```

2. Keyword patterns:
  ```bash
val ketoPattern = "(?i)\\bketo\\b".r
val veganPattern = "(?i)\\bvegan\\b".r

```

3. Spark configuration for memory and performance optimization:

  ```bash
val sparkConf = new SparkConf()
  .setAppName("RUBigDataApp")
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .registerKryoClasses(Array(classOf[WarcRecord]))
  .set("spark.driver.memory", "4g")
  .set("spark.executor.memory", "8g")
  .set("spark.memory.offHeap.enabled", "true")
  .set("spark.memory.offHeap.size", "6g")
```

 ## Output:

1. The top 10 web pages with the highest number of keto mentions:
   - URL
   - Standardized keto count (relative to page size)
   - Percentage representation
   - Raw keto count
   - Total word count

2. The top 10 web pages with the highest number of vegan mentions (same metrics as above)
```bash
########## Start ##########

The best webpages in WARC files mentioning 'keto' (standardized by total word count):
URL: https://example.com/keto-article
Standardized Keto Count: 0.01423
Standardized Keto Count Percentage: 1.42%
Keto Count: 34
Total Word Count: 2388

The best webpages in WARC files mentioning 'vegan' (standardized by total word count):
URL: https://example.com/vegan-guide
Standardized Vegan Count: 0.00931
Standardized Vegan Count Percentage: 0.93%
Vegan Count: 22
Total Word Count: 2365

########### Ending ############

```

## Performance considerations:
- Uses Kryo serializer to reduce serialization overhead
- Configures memory settings for driver and executor
- Utilizes off-heap memory to reduce garbage collection overhead
- Filters out non-English pages to improve processing efficiency
- Implements efficient regex pattern matching
- Includes error handling to prevent task failures from malformed pages

## Future improvements:

Potential enhancements:
- Implementation of context-aware analysis to reduce false positives
- Addition of related diet keywords for more comprehensive results
- Integration with visualization tools
- Sentiment analysis to determine positive vs negative mentions
- Scaling the analysis to 25, 50, or full WARC segments for more robust conclusions

  

