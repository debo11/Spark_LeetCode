package com.bigdata.leetcode

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, datediff, dense_rank, lit, round, when}
import sun.misc.Version.print

object Immediate_Food_Delivery_II {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local").appName("Immediate Food Delivery II").getOrCreate()
    val delivery_input_df = spark.read.format("csv").option("header", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/delivery.csv")
    delivery_input_df.show()

    //using dense_rank to get first order for each customer
    val windowSpec  = Window.partitionBy("customer_id").orderBy("order_date")
    val firstorder_df = delivery_input_df.withColumn("ranking", dense_rank().over(windowSpec)).filter(col("ranking") === 1)
    firstorder_df.show()

    //creating column status column for immediate or scheduled
    val deliverystatus_df = firstorder_df.withColumn("delivery_status", when(datediff(col("customer_pref_delivery_date"),col("order_date")) === 0, lit("immediate")).otherwise(lit("scheduled")))
    deliverystatus_df.show()
    deliverystatus_df.printSchema()

    //finding immediate delivery percentage
    val immediatedeliverycount_df =  deliverystatus_df.groupBy("delivery_status").agg((functions.count("delivery_status")).alias("count"), (functions.count("delivery_status") / deliverystatus_df.count) * 100).filter(col("delivery_status") === "immediate").select(round(col("((count(delivery_status) / 4) * 100)"),2).alias("immediate_percentage"))
    immediatedeliverycount_df.show()


  }
  }
