package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}

class Bank_Account_Summary {
  val spark = SparkSession.builder.master("local").appName("Bank Account Summary").getOrCreate()
  val users_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/users.csv")
  val transactions_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/transaction_v3.csv")
  users_input_df.show()
  transactions_input_df.show()

  //distributing dataframe between paid_by ,paid_to and later merging to get req columns
  val paid_df = transactions_input_df.select(col("trans_id").alias("transaction_id"), col("paid_by").alias("sender"), col("amount").alias("sender_amount"))
  val received_df = transactions_input_df.select(col("trans_id"), col("paid_to").alias("receiver"), col("amount").alias("receiver_amount"))
  val paidreceive_df = paid_df.join(received_df, paid_df("sender") === received_df("receiver"), "left").selectExpr("receiver","sender_amount","receiver_amount").orderBy("receiver")
  paid_df.show()
  received_df.show()
  paidreceive_df.show()

  //joining above dataframes with transaction input to calculate credit score
  val merged_df = users_input_df.join(paidreceive_df, users_input_df("user_id") === paidreceive_df("receiver"), "left")
  val creditcalc_df = merged_df.withColumn("credit_score", col("credit") - col("sender_amount") + col("receiver_amount")).withColumn("credit_calc", when(col("credit_score").isNull, col("credit")).otherwise(col("credit_score")))
  merged_df.show()
  creditcalc_df.show()

  //calculating credit limit breach
  val output_df = creditcalc_df.withColumn("credit_limit_breached", when(col("credit_calc") > 0, lit("No")).otherwise("Yes")).select(col("user_id"),col("user_name"),col("credit_calc").alias("credit"),col("credit_limit_breached"))
  output_df.show()



}


object Bank_Account_Summary {
  def main(args: Array[String]) {
    val Bank_Account_Summary = new Bank_Account_Summary()

    def apply(): Bank_Account_Summary = Bank_Account_Summary

  }
}
