package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, datediff, substring, when, year}

object Total_Sales_Amount_by_Year {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local").appName("Investments in 2016").getOrCreate()
    val df_product_input = spark.read.format("csv").option("header", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/product.csv").withColumnRenamed("product_id","prod_id")
    val df_sales_input = spark.read.format("csv").option("header", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/sales.csv")
    df_product_input.show()
    df_sales_input.show()

    //joining above df
    val df_merged = df_product_input.join(df_sales_input, df_product_input("prod_id") === df_sales_input("product_id"), "inner")
    val df_output = df_merged.withColumn("start_date",col("period_start").cast("date")).withColumn("end_date",col("period_end").cast("date"))
    df_output.show()

    //finding total sales days
    val df_sales_period = df_output.withColumn("total_days", datediff(col("end_date"),col("start_date"))+1)
    val df_yeardiff = df_sales_period.withColumn("yearDifference", year(col("end_date"))-year(col("start_date"))).withColumn("start_year", substring(col("start_date"),1,4)).withColumn("end_year", substring(col("end_date"),1,4)).withColumn("middle_year", when(col("yearDifference") === "2", "2019").otherwise("null"))
    df_yeardiff.show()

    //splitting dataset based on yeardifference
    val sameyear_df = df_yeardiff.selectExpr("prod_id","start_year")
    val oneyeardiff_df = df_yeardiff.selectExpr("prod_id","end_year")
    val twoyeardiff_df = df_yeardiff.selectExpr("prod_id","middle_year")

    val uniondistinct_df = sameyear_df.union(oneyeardiff_df).union(twoyeardiff_df).distinct().filter(col("start_year") =!= "null").orderBy("prod_id").select(col("prod_id"),col("start_year").alias("report_year")).show()
  }
}