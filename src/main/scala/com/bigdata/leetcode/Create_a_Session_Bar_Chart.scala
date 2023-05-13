package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.functions.{col, lit, min, round, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Create_a_Session_Bar_Chart {
  val spark = SparkSession.builder.master("local").appName("Create a Session Bar Chart").getOrCreate()

  //reading inputs
  val session_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/sessions.csv")

  def SessionRange(session_input_df:DataFrame):DataFrame ={
    info("creating column showing mints")
    val mint_df = session_input_df.withColumn("in_minutes", round((col("duration")/60), 2))
    info("categorizing based on minutes range")
    val minutesrange_df = mint_df.withColumn("bin", when(col("in_minutes").between(0,5), lit("[0-5>")).when(col("in_minutes").between(5,10), lit("[5-10>")).when(col("in_minutes").between(10,15), lit("[10-15>")).when(col("in_minutes") > 15, lit("15 or more")).otherwise("NA"))
    val rangecount_df = minutesrange_df.select("bin").groupBy("bin").count().select(col("bin"), col("count").alias("total"))
    rangecount_df
  }

  val output_df = SessionRange(session_input_df)
  output_df.show()

}




object Create_a_Session_Bar_Chart {
  def main(args: Array[String]) {
    val Create_a_Session_Bar_Chart = new Create_a_Session_Bar_Chart()

    def apply(): Create_a_Session_Bar_Chart = Create_a_Session_Bar_Chart

  }
}


