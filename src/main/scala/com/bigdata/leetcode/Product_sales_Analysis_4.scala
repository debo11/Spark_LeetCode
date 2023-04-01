package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}

class Product_sales_Analysis_4 {
  val spark = SparkSession.builder.master("local").appName("Product Sales Analysis IV").getOrCreate()
  val salesinput_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/sales_v2.csv")
  val product_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/product_v2.csv")
  salesinput_df.show()
  product_input_df.show()

  //grouping quanitity for each product for respective customers
  val quantitysum_df = salesinput_df.groupBy("product_id","user_id").sum("quantity").select(col("product_id").alias("prod_id") ,col("user_id"), col("sum(quantity)").alias("total_quantity"))
  quantitysum_df.show()

  //merging above dataframe with product to get total amount for respective products
  val merged_df = quantitysum_df.join(product_input_df, quantitysum_df("prod_id") === product_input_df("product_id"), "inner")
  val totalamount_df = merged_df.withColumn("total_amount", col("total_quantity") * col("price")).orderBy("user_id")
  merged_df.show()
  totalamount_df.show()

  //finding most costly product for each customer
  totalamount_df.createOrReplaceTempView("sales")
  val toproduct_df = spark.sql("select a.* from (select user_id, total_quantity,product_id,total_amount, dense_rank() over (partition by user_id order by total_amount desc) as rnk from sales) a where a.rnk == 1")
  toproduct_df.show()

  //printing resultant
  val output_df = toproduct_df.select("user_id","product_id")
  output_df.show()

}


object Product_sales_Analysis_4 {
  def main(args: Array[String]) {
    val Product_sales_Analysis_4 = new Product_sales_Analysis_4()

    def apply(): Product_sales_Analysis_4 = Product_sales_Analysis_4

  }
}
