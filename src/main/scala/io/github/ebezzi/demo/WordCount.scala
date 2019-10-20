package io.github.ebezzi.demo

object WordCount {

  def main(args: Array[String]): Unit = {

    import org.apache.spark._
    import org.apache.spark.streaming._
    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent a starvation scenario.

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
