package com.cummins.util

import org.apache.spark.sql.SparkSession

/**
 * (对Job的功能描述)
 * Create by sm487 on 2021/8/9
 * READ : [TableName]
 * SAVE : [TableName]
 */
trait Job {
  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sqlContext.setConf("spark.sql.session.timeZone", "UTC")
  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    run()
  }
  def run()
}
