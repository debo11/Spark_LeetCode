package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array_contains, col, collect_list, date_add, lead, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Customers_with_Maximum_Number_of_Transactions_on_Consecutive_Days {

  val spark = SparkSession.builder.master("local").appName("Customers with Maximum Number of Transactions on Consecutive Days").getOrCreate()

  //reading inputs
  val transactions_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/transactions_v1.csv")

  def ConsecutiveTransaction(transactions_input_df: DataFrame): DataFrame = {
    info("using lead to check consecutive dates")
    val windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")
    val consecutive_days_df = transactions_input_df.withColumn("next_day_record", lead("transaction_date",1,"").over(windowSpec))

    info("to check if transaction_date and next_day_record are on next day")
    val nextday_df = consecutive_days_df.withColumn("next_day_check", when(col("next_day_record") === date_add(col("transaction_date") , 1), lit("Yes")).otherwise("No")).filter(col("next_day_record").isNotNull)
    val invalidrecords_df = nextday_df.groupBy("customer_id").agg(collect_list("next_day_check")).select(col("customer_id").alias("cust_id"),col("collect_list(next_day_check)").alias("combo")).where(array_contains(col("combo"),"No"))
    val final_df = nextday_df.join(invalidrecords_df, nextday_df("customer_id") === invalidrecords_df("cust_id"), "left_anti").select("customer_id").distinct()
    final_df



  }

  val output_df = ConsecutiveTransaction(transactions_input_df)
  output_df.show()

}

object Customers_with_Maximum_Number_of_Transactions_on_Consecutive_Days {
  def main(args: Array[String]) {
    val Customers_with_Maximum_Number_of_Transactions_on_Consecutive_Days = new Customers_with_Maximum_Number_of_Transactions_on_Consecutive_Days()

    def apply(): Customers_with_Maximum_Number_of_Transactions_on_Consecutive_Days = Customers_with_Maximum_Number_of_Transactions_on_Consecutive_Days

  }
}