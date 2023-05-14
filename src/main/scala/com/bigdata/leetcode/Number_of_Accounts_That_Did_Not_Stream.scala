package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.functions.{col, lit, substring, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Number_of_Accounts_That_Did_Not_Stream {
  val spark = SparkSession.builder.master("local").appName("Number of Accounts That Did Not Stream").getOrCreate()

  //reading inputs
  val subscription_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/subscriptions.csv")
  val stream_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/streams.csv")
  subscription_input_df.show()
  stream_input_df.show()

  def StreamAccount(subscription_input_df:DataFrame, stream_input_df:DataFrame):DataFrame ={
    info("finding accounts who had subscription till 2021 and did streaming year respectively")
    val subscribed2021_df = subscription_input_df.withColumn("subscription_end_year", substring(col("end_date"),1,4)).select(col("account_id").alias("acc_id"), col("subscription_end_year"))
    val stream2021_df = stream_input_df.withColumn("stream_year", substring(col("stream_date"),1,4))
    val merged_df = stream2021_df.join(subscribed2021_df, stream2021_df("account_id") === subscribed2021_df("acc_id"), "left")

    info("finding accounts did not stream")
    val nonstreaming_df = merged_df.withColumn("stream_in_2021", when(col("stream_year") === col("subscription_end_year"), lit("Valid")).otherwise("Invalid"))
    val invalidaccount_df = nonstreaming_df.groupBy("stream_in_2021").count().select(col("stream_in_2021"), col("count").alias("accounts_count")).filter(col("stream_in_2021") === "Invalid").select("accounts_count")
    invalidaccount_df

  }

  val output_df = StreamAccount(subscription_input_df,stream_input_df)
  output_df.show()
}


object Number_of_Accounts_That_Did_Not_Stream {
  def main(args: Array[String]) {
    val Number_of_Accounts_That_Did_Not_Stream = new Number_of_Accounts_That_Did_Not_Stream()

    def apply(): Number_of_Accounts_That_Did_Not_Stream = Number_of_Accounts_That_Did_Not_Stream

  }
}