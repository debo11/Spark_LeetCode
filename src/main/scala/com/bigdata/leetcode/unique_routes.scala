package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.functions.{col, collect_list}
import org.apache.spark.sql.{DataFrame, SparkSession}

class unique_routes {


  val spark = SparkSession.builder.master("local").appName("Cooking Recipes").getOrCreate()

  //reading inputs
  val routes_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/routes.csv")

  def unique_routes(routes_input_df: DataFrame): DataFrame = {
    info("adding both columns value in list")
    val combination_df = routes_input_df.withColumn("combination", collect_list(col("src")))
    combination_df

  }


  val output_df = unique_routes(routes_input_df)
  output_df.show()
}



object unique_routes {
  def main(args: Array[String]) {
    val unique_routes = new unique_routes()

    def apply(): unique_routes = unique_routes

  }
}
