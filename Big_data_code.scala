package org.rubigdata

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat, WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.SparkConf
import org.jsoup.Jsoup
import scala.collection.JavaConverters._
import org.apache.spark.sql._

object RUBigDataApp {
  def main(args: Array[String]) {

    // First we need to configure Spark 
    val sparkConf = new SparkConf()
      .setAppName("RUBigDataApp")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[WarcRecord]))
      .set("spark.driver.memory", "4g")
      .set("spark.executor.memory", "8g")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "6g")

    implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    // Defining the WARC files to read
    val warcFiles = (0 to 1).map(a => f"hdfs:/single-warc-segment/CC-MAIN-20210410105831-20210410135831-$a%05d.warc.gz")
    
    // Patterns for "keto" and "vegan" keywords
    val ketoPattern = "(?i)\\bketo\\b".r
    val veganPattern = "(?i)\\bvegan\\b".r

    // Extracting the body of the websites and finding the appearances of "keto" and "vegan"
    def processWarcRecord(wr: WarcRecord): (String, Double, Double, Int, Int, Int) = {
      try {
        val bodyText = Jsoup.parse(wr.getHttpStringBody()).select("body").text().toLowerCase
        val totalWordCount = bodyText.split("\\s+").length
        val ketoCount = ketoPattern.findAllIn(bodyText).toList.length
        val veganCount = veganPattern.findAllIn(bodyText).toList.length
        val standardizedKetoCount = if (totalWordCount > 0) ketoCount.toDouble / totalWordCount else 0.0
        val standardizedVeganCount = if (totalWordCount > 0) veganCount.toDouble / totalWordCount else 0.0
        (wr.getHeader().getUrl(), standardizedKetoCount, standardizedVeganCount, ketoCount, veganCount, totalWordCount)
      } catch {
        case e: Exception =>
          println(s"Error processing record: ${e.getMessage}")
          ("", 0.0, 0.0, 0, 0, 0)
      }
    }

    // Reading and processing WARC files
    val warcRecords = sc.newAPIHadoopFile(
      warcFiles.mkString(","),
      classOf[WarcGzInputFormat],
      classOf[NullWritable],
      classOf[WarcWritable]
    ).map { wr => wr._2.getRecord() }
      .filter(wr => wr.getHeader().getUrl() != null && wr.getHttpStringBody() != null)
      .filter(wr => {
        val doc = Jsoup.parse(wr.getHttpStringBody())
        val lang = doc.select("html").attr("lang")
        lang == null || lang.isEmpty || lang.startsWith("en") // Filtering by language (only English resources)
      })

    val processedRecords = warcRecords.map(processWarcRecord).filter(r => r._4 > 0 || r._5 > 0)

    // Here we create the dataframe to store the obtained results,such that we can analyse them later 
    val df = spark.createDataFrame(processedRecords).toDF("url", "standardized_keto_count", "standardized_vegan_count", "keto_count", "vegan_count", "total_wordcount")
    df.createOrReplaceTempView("df")

    // Query to get the top 10 web pages by problem count
    val topKetoPages = spark.sql("SELECT url, standardized_keto_count, keto_count, total_wordcount FROM df WHERE keto_count > 0 ORDER BY keto_count DESC LIMIT 10").collect()
    val topVeganPages = spark.sql("SELECT url, standardized_vegan_count, vegan_count, total_wordcount FROM df WHERE vegan_count > 0 ORDER BY vegan_count DESC LIMIT 10").collect()

    // Printing the final results
    println("\n########## Start ##########")
    println("The best webpages in WARC files mentioning 'keto' (standardized by total word count):")
    topKetoPages.foreach { row =>
      val standardizedKetoCount = row.getDouble(1)
      val standardizedPercentage = standardizedKetoCount * 100 // Convert to percentage
      println(s"URL: ${row.getString(0)}")
      println(f"Standardized Keto Count: $standardizedKetoCount%.5f") // Display as is
      println(f"Standardized Keto Count Percentage: $standardizedPercentage%.2f%%") // Display as percentage
      println(s"Keto Count: ${row.getInt(2)}")
      println(s"Total Word Count: ${row.getInt(3)}")
      println()
    }

    println("The best webpages in WARC files mentioning 'vegan' (standardized by total word count):")
    topVeganPages.foreach { row =>
      val standardizedVeganCount = row.getDouble(1)
      val standardizedPercentage = standardizedVeganCount * 100 // Convert to percentage
      println(s"URL: ${row.getString(0)}")
      println(f"Standardized Vegan Count: $standardizedVeganCount%.5f") // Display as is
      println(f"Standardized Vegan Count Percentage: $standardizedPercentage%.2f%%") // Display as percentage
      println(s"Vegan Count: ${row.getInt(2)}")
      println(s"Total Word Count: ${row.getInt(3)}")
      println()
    }
    println("########### Ending ############\n")

    spark.stop()
  }
}
