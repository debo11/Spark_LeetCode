package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class warehouse_manager {

  val spark = SparkSession.builder.master("local").appName("Warehouse Manager").getOrCreate()

  //reading inputs
  val warehouse_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/warehouse.csv")
  val products_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/products_v1.csv")

  def warehouse_manager(warehouse_input_df: DataFrame , products_input_df: DataFrame): DataFrame = {
    info("finding volume of respective products")
    val volumeDF = products_input_df.withColumn("volume_calculated", col("width") * col("length") * col("height")).withColumnRenamed("product_id","prod_id")

    info("joining above dataframes")
    val mergedDF = warehouse_input_df.join(volumeDF, warehouse_input_df("product_id") === volumeDF("prod_id"), "left")

    info("calculating total volume for each units")
    val volumeunitDF = mergedDF.withColumn("total_volume", col("units") * col("volume_calculated"))

    info("aggregating based on name total volume in warehouse")
    val outputDF = volumeunitDF.select("name", "total_volume").groupBy("name").sum("total_volume").select(col("name").alias("warehouse_name"), col("sum(total_volume)").alias("volume")).orderBy("name")
    outputDF


  }

  val output_df = warehouse_manager(warehouse_input_df,products_input_df)
  output_df.show()

}


object warehouse_manager {
  def main(args: Array[String]) {
    val warehouse_manager = new warehouse_manager()

    def apply(): warehouse_manager = warehouse_manager

  }
}
