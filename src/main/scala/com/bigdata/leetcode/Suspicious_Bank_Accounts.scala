package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, lag, lead, lit, substring, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Suspicious_Bank_Accounts {
  val spark = SparkSession.builder.master("local").appName("Suspicious Bank Accounts").getOrCreate()

  //reading inputs
  val accounts_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/accounts_v2.csv")
  val transactions_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/transactions.csv")
  accounts_input_df.show()
  transactions_input_df.show()

  def BankAccount (accounts_input_df:DataFrame, transactions_input_df:DataFrame):DataFrame={
    info("finding period and total transaction on monthly basis")
    val period_df = transactions_input_df.withColumn("period", substring(col("day"),1,7)).filter(col("type") === "Creditor").orderBy("account_id","day").withColumnRenamed("account_id","acc_id")
    val transaction_df = period_df.select("acc_id","period","amount").groupBy("acc_id","period").sum("amount").select(col("acc_id").alias("account_ids"),col("period").alias("periods"),col("sum(amount)").alias("transaction_amount"))
    period_df

    info("merging accounts table with above dataframe")
    val merged_df = period_df.join(transaction_df, period_df("acc_id") === transaction_df("account_ids") && period_df("period") === transaction_df("periods"), "left")
    val maxincomemerged_df = merged_df.join(accounts_input_df, merged_df("acc_id") === accounts_input_df("account_id"), "left").select("account_ids","period","transaction_amount","max_income").distinct().orderBy("account_ids","period")
    maxincomemerged_df

    info("calculating transaction between months and continuous 2 suspicious streak to find the resultant")
    val suspicious_df = maxincomemerged_df.withColumn("suspicious_check", when(col("transaction_amount") > col("max_income"), lit("Suspicious")).otherwise("Non-Suspicious"))
    val windowSpec = Window.partitionBy("account_ids").orderBy(col("period"))
    val ranking_df = suspicious_df.withColumn("rank", dense_rank().over(windowSpec))
    ranking_df

    //use dense rank over colum suspicious_check to find valid suspicious accounts
    val windowSpec2  = Window.partitionBy("account_ids").orderBy("period")
    val validcustomer_df = ranking_df.withColumn("next_month", lead("suspicious_check", 1).over(windowSpec2)).filter(col("next_month").isNotNull)
    val finaloutput_df = validcustomer_df.filter(col("suspicious_check") === col("next_month")).select("account_ids").distinct()
    finaloutput_df

  }

  val output_df = BankAccount(accounts_input_df,transactions_input_df)
  output_df.show()

}


object Suspicious_Bank_Accounts {
  def main(args: Array[String]) {
    val Suspicious_Bank_Accounts = new Suspicious_Bank_Accounts()

    def apply(): Suspicious_Bank_Accounts = Suspicious_Bank_Accounts

  }
}