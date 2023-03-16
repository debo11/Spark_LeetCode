package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}

class Active_Users {
  val spark = SparkSession.builder.master("local").appName("Active Users").getOrCreate()
  val accounts_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/accounts.csv")
  val login_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/logins.csv")
  accounts_input_df.show()
  login_input_df.show()

  //finding users for consecutive 5 days defining as Active
  val windowSpec = Window.partitionBy("id").orderBy("login_date")
  val activeusercheck_df = login_input_df.withColumn("rank", dense_rank().over(windowSpec))

  val activecheck_df = activeusercheck_df.filter(col("rank") === 5).select(col("id").alias("id_no")).distinct()
  val output_df = activecheck_df.join(accounts_input_df, activecheck_df("id_no") === accounts_input_df("id"), "left").selectExpr("id","name")

  output_df.show()

}



object Active_Users {
  def main(args: Array[String]) {
    val Active_Users = new Active_Users()

    def apply(): Active_Users = Active_Users

  }
}