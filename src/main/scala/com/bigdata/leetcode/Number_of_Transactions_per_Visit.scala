package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object Number_of_Transactions_per_Visit {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local").appName("Number of Transactions per Visit").getOrCreate()
    val visit_input_df = spark.read.format("csv").option("header", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/visits.csv")
    val transaction_input_df = spark.read.format("csv").option("header", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/transaction_2.csv").withColumnRenamed("user_id","users_id")
    visit_input_df.show()
    transaction_input_df.show()

    //merging above dataframes
    val merged_df = visit_input_df.join(transaction_input_df, visit_input_df("user_id") === transaction_input_df("users_id") && visit_input_df("visit_date") === transaction_input_df("transaction_date"), "left")
      merged_df.show()

    //taking count per user and transaction
    val transusercount_df = merged_df.groupBy("users_id", "transaction_date").count().select(col("users_id"),col("transaction_date"),col("count").alias("transaction_count")).orderBy("users_id")
    transusercount_df.show()

    //filtering out users who has 0 visits
    val zerovisit_df = transusercount_df.filter("transaction_date is Null or users_id is Null" ).withColumnRenamed("transaction_count","visits_count").withColumn("transaction_count",lit("0")).groupBy("transaction_count").sum("visits_count").select(col("transaction_count"),col("sum(visits_count)").alias("visits_count"))
      zerovisit_df.show()

    //taking sum based on transaction_count
    val visitcount_df = transusercount_df.filter("transaction_date is not Null" ).selectExpr("transaction_count").groupBy("transaction_count").count().select(col("transaction_count"),col("count").alias("visits_count"))
      visitcount_df.show()

    //merging above dataframe to get resultant
    val output_df = zerovisit_df.union(visitcount_df)
    output_df.show()
  }
}
