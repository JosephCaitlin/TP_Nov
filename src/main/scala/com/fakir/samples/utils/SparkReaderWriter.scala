package com.fakir.samples.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkReaderWriter {
  def readData(inputPath: String, inputFormat: String): DataFrame = {
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    /*inputFormat match {
      case "CSV" => sparkSession.read.csv(inputPath)
      case "parquet" => sparkSession.read.parquet(inputPath)
      case _ => sparkSession.read.csv(inputPath)
    }*/
    if(inputFormat == "CSV")
      sparkSession.read.option("header", true).option("inferSchema", true).csv(inputPath)
    else {
      sparkSession.read.parquet(inputPath)
    }
  }

  def writeData(df: DataFrame, outputPath: String, outputFormat: String, partitions: Seq[String]) = {
    if(outputFormat == "CSV") {
      df.write.partitionBy(partitions:_*).csv(outputPath)
    }
    else {
      df.write.partitionBy(partitions:_*).parquet(outputPath)
    }
  }


}
