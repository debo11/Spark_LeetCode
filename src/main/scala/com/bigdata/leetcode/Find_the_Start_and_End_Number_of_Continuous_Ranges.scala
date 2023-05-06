package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}

class Find_the_Start_and_End_Number_of_Continuous_Ranges {
  val spark = SparkSession.builder.master("local").appName("Find the Start and End Number of Continuous Ranges).getOrCreate()

  //reading inputs
  val logs_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/logs_v2.csv")

  def ContinuousRanges (activity_input_df: DataFrame): DataFrame = {
    info("finding missing logs and ranges ")
    logs_input_df.createOrReplaceTempView("logs")
    val ranges_df = spark.sql("select a.log_id as start_log, b.log_id as end_log from logs a join logs b where a.log_id <= b.log_id AND a.log_id - 1 not in (SELECT * FROM Logs) AND b.log_id + 1 not in (SELECT * FROM Logs) Group by a.log_id,b.log_id")
    val windowSpec = Window.partitionBy("start_log").orderBy(col("end_log"))
    val finaloutput_df = ranges_df.withColumn("ranking", dense_rank().over(windowSpec)).filter(col("ranking") === 1).select("start_log","end_log")
    finaloutput_df

  }

  val output_df = ContinuousRanges(logs_input_df)
  output_df.show()


}


object Find_the_Start_and_End_Number_of_Continuous_Ranges {
  def main(args: Array[String]) {
    val Find_the_Start_and_End_Number_of_Continuous_Ranges = new Find_the_Start_and_End_Number_of_Continuous_Ranges()

    def apply(): Find_the_Start_and_End_Number_of_Continuous_Ranges = Find_the_Start_and_End_Number_of_Continuous_Ranges

  }
}