package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class Capital_Gain_Loss {
  val spark = SparkSession.builder.master("local").appName("Capital Gain Loss").getOrCreate()
  val stocks_input_df = spark.read.format("csv").option("header", "true").option("InferSchema","true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/stocks.csv")
  stocks_input_df.show()
  stocks_input_df.printSchema()

  //distribute dataframe based on buy and sell and find the agg amount

  val buy_df = stocks_input_df.filter(col("operation") === "Buy").groupBy("stock_name").sum("price").select(col("stock_name").alias("stock"),col("sum(price)").alias("buy_amount"))
  val sell_df = stocks_input_df.filter(col("operation") === "Sell").groupBy("stock_name").sum("price").select(col("stock_name"),col("sum(price)").alias("sell_amount"))

  //merging above df and finding capital_gain_loss

  val merge_df = sell_df.join(buy_df, sell_df("stock_name") === buy_df("stock"), "inner").selectExpr("stock_name","buy_amount","sell_amount")
  val output_df = merge_df.withColumn("capital_gain_loss", col("sell_amount") - col("buy_amount")).selectExpr("stock_name","capital_gain_loss")

  output_df.show()
}


object Capital_Gain_Loss {
  def main(args: Array[String]) {
    val Capital_Gain_Loss = new Capital_Gain_Loss()

    def apply(): Capital_Gain_Loss = Capital_Gain_Loss

  }
}