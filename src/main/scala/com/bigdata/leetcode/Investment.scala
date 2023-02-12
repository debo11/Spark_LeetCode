package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, max, row_number}

object Investment {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local").appName("Investments in 2016").getOrCreate()
    val df_input = spark.read.format("csv").option("header","true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/insurance.csv")
    df_input.show()

   //finding the same tiv_2015 occurance
      val prevtiv_df = df_input.select("pid","tiv_2015").groupBy("tiv_2015").count()
    prevtiv_df.createOrReplaceTempView("fb")
    val mostoccurance_df = spark.sql("select tiv_2015 as prev_tiv,count as max_occurance from " + " (select tiv_2015,count, DENSE_RANK() OVER ( ORDER BY count desc) as rnk " + " FROM fb) tmp where rnk = 1")
    mostoccurance_df.show()

    //finding unique long,lat pair
    val windowSpec = Window.partitionBy("lat","lon").orderBy("pid")
    val unique_df = df_input.withColumn("ranking", dense_rank().over(windowSpec)).filter(col("ranking") === 1)
    unique_df.show()

    //merging above df
    val merged_df = mostoccurance_df.join(unique_df, mostoccurance_df("prev_tiv") === unique_df("tiv_2015"), "inner")
    val df_output = merged_df.withColumn("inv_2016",col("tiv_2016").cast("numeric"))
    val finalOutput_df = df_output.groupBy("tiv_2015").sum("inv_2016").select(col("sum(inv_2016)").alias("investment_2016"))
    finalOutput_df.show()


  }
}

