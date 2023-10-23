package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, count, lag, lit, round, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class confirmation_rate {


  val spark = SparkSession.builder.master("local").appName("Confirmation Rate").getOrCreate()

  //reading inputs
  val signups_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/signups.csv").withColumnRenamed("user_id","users_id").withColumnRenamed("time_stamp","time_logged")
  val confirmations_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/confirmation.csv")

  def ConfirmationRate(signups_input_df: DataFrame,confirmations_input_df:DataFrame): DataFrame = {
    info("joining confirmation with signups to get valid users")
    val validusers_df = signups_input_df.join(confirmations_input_df, signups_input_df("users_id") === confirmations_input_df("user_id"), "right")

    info("finding total sessions based on each users")
    val sessioncount_df = validusers_df.select("users_id").groupBy("users_id").count().select(col("users_id").alias("usr_id"), col("count").alias("session_count"))

    info("finding distinct actions for each users")
   val usersactions_confirm_count_df = validusers_df.select("users_id","action").filter(col("action")=== "confirmed").groupBy("users_id","action").count().select(col("users_id").alias("id"), col("count").alias("confirmation_count"))

    info("merging above dataframes")
    val merged_df = sessioncount_df.join(usersactions_confirm_count_df, sessioncount_df("usr_id") === usersactions_confirm_count_df("id"), "left").select("usr_id", "session_count","confirmation_count")

    info("merging above dataframe with signups table")
    val finaloutput_df = signups_input_df.join(merged_df, signups_input_df("users_id") === merged_df("usr_id"), "left").select("users_id","session_count","confirmation_count")
    val confirmationrate_df = finaloutput_df.withColumn("confirmation_rate", when( col("session_count").isNotNull && col("confirmation_count").isNotNull ,round(col("confirmation_count") / col("session_count"),2)).when(col("session_count").isNull || col("confirmation_count").isNull, lit("0.00")).otherwise("NA")).select("users_id","confirmation_rate")

    confirmationrate_df

  }

  val output_df = ConfirmationRate(signups_input_df,confirmations_input_df)
  output_df.show()

}


object confirmation_rate {
  def main(args: Array[String]) {
    val confirmation_rate = new confirmation_rate()

    def apply(): confirmation_rate = confirmation_rate

  }
}