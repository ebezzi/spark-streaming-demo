package io.github.ebezzi.demo

import org.apache.spark.sql.{DataFrame, SaveMode}

object InvertedIndex {

  def main(args: Array[String]): Unit = { 

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val files = spark.readStream
      .option("wholetext", "true")
      .text("file:///opt/spark-streaming-demo/data")
      .withColumn("document", input_file_name())
      .withColumn("text", lower(col("value")))


    val idx = files
      .withColumn("words", split(col("text"), "[\\p{Punct}\\s]+"))
      .withColumn("word", explode(col("words")))
      .where(expr("word is not null and length(word) > 0"))
      .groupBy("word")
      .agg(collect_set(col("document")).as("docs"))
      .withColumn("df", size(col("docs")))

    val consoleSink = idx.writeStream
      .outputMode("update")
      .format("console")
      .start()

    import org.apache.spark.sql.cassandra._

    val redisSink = idx.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF
          .select("word", "docs")
          .write
          .cassandraFormat("idx", "demo")
          .mode(SaveMode.Append)
          .save()
      }
      .start()

    consoleSink.awaitTermination()
    redisSink.awaitTermination()

  }

}
