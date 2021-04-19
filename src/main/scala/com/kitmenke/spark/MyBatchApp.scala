package com.kitmenke.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object MyBatchApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "MyApp"

  def main(args: Array[String]): Unit = {

    try {
      val spark = SparkSession.builder()
        .appName(jobName)
        .config("spark.sql.shuffle.partitions", "3")
        .master("local[*]")
        .getOrCreate()
      import spark.implicits._

      val sentences = spark.read.csv("src/main/resources/sentences.txt").as[String]
      sentences.printSchema

      val words = sentences.flatMap(word => {word.split(" ")})
      val lowercase = words.map(word => word.toLowerCase)
      val noPeriods = lowercase.map(word => word.replaceAll("[^A-Za-z]", ""))
      val grouping = noPeriods.groupBy("value").count()
      
      grouping.foreach(wordCount=>println(wordCount))
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }


}
