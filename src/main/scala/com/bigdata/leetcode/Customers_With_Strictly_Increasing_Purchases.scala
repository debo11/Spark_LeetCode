package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, lead, lit, substring, when}

class Customers_With_Strictly_Increasing_Purchases {
  val spark = SparkSession.builder.master("local").appName("Customers With Strictly Increasing Purchases").getOrCreate()
  val orders_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/orders.csv")
  orders_input_df.show()

  //extracting year for each customer
  var year_df = orders_input_df.withColumn("order_year", substring(col("order_date"),1,4))
  year_df.show()

  //finding total amount based on year
  var totalamountperyear_df = year_df.select("customer_id","price","order_year").groupBy("customer_id","order_year").sum("price").orderBy("customer_id","order_year").select(col("customer_id"),col("order_year"),col("sum(price)").alias("total_amount"))
    totalamountperyear_df.show()

  //using lag function to get calculate amount over the year and check if increasing or not
  val windowSpec  = Window.partitionBy("customer_id").orderBy("order_year")
  val windowSpec1  = Window.partitionBy("customer_id").orderBy("order_date")
  val amountcheck_df = totalamountperyear_df.withColumn("next_year_amount", lag("total_amount", 1).over(windowSpec)).where(col("next_year_amount").isNotNull)
  val invalidpurchaseincreasecheck_df = amountcheck_df.withColumn("amount_check", when(col("total_amount") - col("next_year_amount") > 0, lit("Valid")).otherwise("InValid")).filter(col("amount_check") === "InValid").select(col("customer_id").alias("cust_id")).distinct()
  invalidpurchaseincreasecheck_df.show()

  //to calculate if there is any year gap between orders for respective customers
  val yeargap_check = orders_input_df.withColumn("nextorder_date", lead("order_date",1).over(windowSpec1)).where(col("nextorder_date").isNotNull)
  val yeardiff_df = yeargap_check.withColumn("previousorder_year", substring(col("order_date"),1,4)).withColumn("nextorder_year", substring(col("nextorder_date"),1,4))
  val invalidcustomeryear_df = yeardiff_df.withColumn("year_check", when(col("previousorder_year") === col("nextorder_year") || col("nextorder_year") === col("previousorder_year") +1, lit("Valid")).otherwise("Invalid")).filter(col("year_check") === "Invalid").select(col("customer_id").alias("cust_id")).distinct()
  invalidcustomeryear_df.show()

  //merge above dataframe to get Invalid customer_id
  val invalid_df = invalidpurchaseincreasecheck_df.union(invalidcustomeryear_df).distinct()
  invalid_df.show()

  //finding customer who have increasing purchase over the year
  val output_df = orders_input_df.select("customer_id").distinct().join(invalid_df, orders_input_df("customer_id") === invalid_df("cust_id"), "left_anti")
  output_df.show()



}


object Customers_With_Strictly_Increasing_Purchases {
  def main(args: Array[String]) {
    val Customers_With_Strictly_Increasing_Purchases = new Customers_With_Strictly_Increasing_Purchases()

    def apply(): Customers_With_Strictly_Increasing_Purchases = Customers_With_Strictly_Increasing_Purchases

  }
}
