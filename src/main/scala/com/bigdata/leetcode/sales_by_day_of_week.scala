package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}

class sales_by_day_of_week {

  val spark = SparkSession.builder.master("local").appName("Sales by day of week").getOrCreate()

  //reading inputs
  val orders_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/orders_v1.csv")
  val items_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/items.csv")

  def SalesbyDayofWeek (orders_input_df: DataFrame, items_input_df: DataFrame): DataFrame = {

    info("reading inputs from orders and finding out on which day of week the order was created")
    val dayDF = orders_input_df.withColumn("week_day", date_format(col("order_date"), "EEEE"))

    info("aggregating quantity based on week_day")
    val quantittyWeekBasisDF = dayDF.select("quantity", "week_day", "item_id").groupBy("week_day", "item_id").sum("quantity").select(col("week_day"), col("item_id").alias("ite_id"), col("sum(quantity)").alias("total_quantity"))

    info("merging above dataframes to get category name")
    val mergedDF = quantittyWeekBasisDF.join(items_input_df, quantittyWeekBasisDF("ite_id") === items_input_df("item_id"), "inner")

    info("pivoting dataframe to get resultant")
    val pivotDF = mergedDF.groupBy("item_category").pivot("week_day").sum("total_quantity")
    val outputDF = pivotDF.na.fill(0.0)
    outputDF


  }

val output_df = SalesbyDayofWeek(orders_input_df, items_input_df)
output_df.show()

}

object sales_by_day_of_week {
  def main(args: Array[String]) {
    val sales_by_day_of_week = new sales_by_day_of_week()

    def apply(): sales_by_day_of_week = sales_by_day_of_week

  }
}
