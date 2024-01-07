package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{bround, col, lag, substring}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Monthly_Percentage_Difference {

  val spark = SparkSession.builder.master("local").appName("Monthly Percentage Difference").getOrCreate()

  //reading inputs
  val transactions_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/sf_transactions.csv")


  def PercentageDiff(transactions_input_df: DataFrame): DataFrame = {

    info("creating year-month format column")
    val yearmonthDf = transactions_input_df.withColumn("year_month", substring(col("created_at"),1,7))

    info("finding monthly total reveneue")
    val monthlyrevenue = yearmonthDf.groupBy("year_month").sum("value").orderBy("year_month")select(col("year_month"), col("sum(value)").alias("monthly_revenue"))
    monthlyrevenue

    info("creating previous_month_revenue column")
   val windowSpec  = Window.orderBy("year_month")
    val previousmonthrevenueDf = monthlyrevenue.withColumn("previous_month_revenue",lag("monthly_revenue",1).over(windowSpec))

    info("finding revenue percentage difference")
    val reveneuediffDF = previousmonthrevenueDf.withColumn("revenue_diff_pct", ((col("monthly_revenue") - col("previous_month_revenue")) / col("previous_month_revenue") ) * 100).select(col("year_month"), bround(col("revenue_diff_pct"),2))
    reveneuediffDF

  }

  val output_df = PercentageDiff(transactions_input_df)
  output_df.show()

}




object Monthly_Percentage_Difference {
  def main(args: Array[String]) {
    val Monthly_Percentage_Difference = new Monthly_Percentage_Difference()

    def apply(): Monthly_Percentage_Difference = Monthly_Percentage_Difference

  }
}
