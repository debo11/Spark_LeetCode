package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, datediff, lit, substring, when, year}

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

    val uniondistinct_df = sameyear_df.union(oneyeardiff_df).union(twoyeardiff_df).distinct().filter(col("start_year") =!= "null").orderBy("prod_id").select(col("prod_id"),col("start_year").alias("report_year"))
    uniondistinct_df.show()

  //merging above df
    val union_df = uniondistinct_df.join(df_yeardiff, uniondistinct_df("prod_id") === df_yeardiff("prod_id"),"inner").orderBy("product_id")
    union_df.show()

    //logic for total days per year
    val daysperyear_df = union_df.withColumn("year_days",when(col("yearDifference") === "0", col("total_days")).when(col("yearDifference") === "1" && col("report_year") === col("start_year"), datediff(concat(col("start_year"),lit("-12-31")) ,col("start_date"))+1).when(col("yearDifference") === "1" && col("report_year") === col("end_year"), datediff(col("end_date"), concat(col("end_year"),lit("-01-01")) )+1).when(col("yearDifference") === "2" && col("report_year") === col("start_year"), datediff(concat(col("start_year"),lit("-12-31")) ,col("start_date"))+1).when(col("yearDifference") === "2" && col("report_year") === col("middle_year"), 365).when(col("yearDifference") === "2" && col("report_year") === col("end_year"), datediff(col("end_date"), concat(col("end_year"),lit("-01-01")) )+1).otherwise("null")).selectExpr("product_id","product_name","report_year","average_daily_sales","year_days")
    daysperyear_df.show()

    //sales per year calculation
    val salescalc_df = daysperyear_df.withColumn("total_amount", col("average_daily_sales") * col("year_days")).selectExpr("product_id","product_name","report_year","total_amount")
    salescalc_df.show()
  }
}