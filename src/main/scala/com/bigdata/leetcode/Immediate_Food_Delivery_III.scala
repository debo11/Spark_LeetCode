package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.functions.{col, lit, round, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Immediate_Food_Delivery_III {
  val spark = SparkSession.builder.master("local").appName("Immediate Food Delivery III").getOrCreate()

  //reading inputs
  val delivery_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/delivery_v2.csv").orderBy("order_date")


  def SameDateDeilivery(delivery_input_df: DataFrame): DataFrame = {
    info("finding same order and delivery date records")
    val samedate_df = delivery_input_df.withColumn("same_date_check", when(col("order_date") === col("customer_pref_delivery_date"),lit(1)).otherwise(lit(0)))
    val order_delivered_df = samedate_df.groupBy("order_date").sum("same_date_check").select(col("order_date").alias("orders_date"), col("sum(same_date_check)").alias("order_count_delivered"))
    val order_count = samedate_df.groupBy("order_date").count().select(col("order_date").alias("order_date"), col("count").alias("order_count_received"))
    val merge_df = order_delivered_df.join(order_count, order_delivered_df("orders_date") === order_count("order_date"), "inner").select("order_date","order_count_delivered","order_count_received")
    val immediate_percentage_df = merge_df.withColumn("immediate_percentage", round((col("order_count_delivered") / col("order_count_received")) * 100,2)).select("order_date","immediate_percentage")
    immediate_percentage_df



  }


val output_df = SameDateDeilivery(delivery_input_df)
output_df.show()

}

object Immediate_Food_Delivery_III {
  def main(args: Array[String]) {
    val Immediate_Food_Delivery_III = new Immediate_Food_Delivery_III()

    def apply(): Immediate_Food_Delivery_III = Immediate_Food_Delivery_III

  }
}