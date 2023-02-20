package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring}

object Monthly_Transactions {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local").appName("Monthly Transactions 1").getOrCreate()
    val transaction_input_df = spark.read.format("csv").option("header", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/transaction.csv")
    transaction_input_df.show()

    //finding transaction counts
    val transaction_count_df = transaction_input_df.withColumn("period", substring(col("trans_date"),4,10))
    val casted_df = transaction_count_df.withColumn("total_amount" , transaction_count_df.col("amount").cast("numeric"))
    val transactioncount_df = casted_df.groupBy("country","period").count().select(col("country"),col("period"),col("count").alias("trans_count")).orderBy("period").withColumnRenamed("country","countries").withColumnRenamed("period","time_period")
    transactioncount_df.show()

    //finding approved count per period and country
    val approved_df = casted_df.filter(col("state") === "approved")
    val approvecount_df = approved_df.groupBy("country","period", "state").count().select(col("country"),col("period"),col("count").alias("approved_count")).orderBy("period")
    approvecount_df.show()

    //finding transaction total amount per year
    val transperyear_df = casted_df.groupBy("country", "period").sum("total_amount").select(col("country"),col("period"),col("sum(total_amount)").alias("trans_total_amount"))
      transperyear_df.show()

    //finding total approved amount per year
    val approvedamount_df = approved_df.groupBy("country","period", "state").sum("total_amount").select(col("country"),col("period"),col("sum(total_amount)").alias("approved_total_amount"))
      approvedamount_df.show()

    //merging above dataframes to get resultant
    val merged_df1 = transactioncount_df.join(approvecount_df, transactioncount_df("countries") === approvecount_df("country") && transactioncount_df("time_period") === approvecount_df("period"), "inner").selectExpr("countries","time_period","trans_count","approved_count")
    val merged_df2 = merged_df1.join(transperyear_df, merged_df1("countries") === transperyear_df("country") && merged_df1("time_period") === transperyear_df("period"), "inner").selectExpr("countries","time_period","trans_count","approved_count","trans_total_amount")
    val merged_df3 = merged_df2.join(approvedamount_df, merged_df2("countries") === approvedamount_df("country") && merged_df2("time_period") === approvedamount_df("period"), "inner").selectExpr("countries","time_period","trans_count","approved_count","trans_total_amount","approved_total_amount")
    merged_df3.show()

  }
}
