package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}

class Generate_the_Invoice {
  val spark = SparkSession.builder.master("local").appName("Generate the Invoice").getOrCreate()
  val products_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/products.csv")
  val purchases_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/pruchases.csv").withColumnRenamed("product_id","prod_id")
  products_input_df.show()
  purchases_input_df.show()

  //merging above dataframes to get price for respective invoices
  val merged_df = products_input_df.join(purchases_input_df, products_input_df("product_id") === purchases_input_df("prod_id"), "inner")
  val totalamount_df = merged_df.withColumn("total_amount", col("price") * col("quantity"))
  val invoiceagg_df = totalamount_df.select("invoice_id","total_amount").groupBy("invoice_id").sum("total_amount").select(col("invoice_id"), col("sum(total_amount)").alias("total_transaction_amount"))
  invoiceagg_df.show()

  //finding invoice with highest transactions
  val windowSpec = Window.orderBy(col("total_transaction_amount").desc)
  val highestinvoicetransaction_df = invoiceagg_df.withColumn("amount_ranking", dense_rank().over(windowSpec)).filter(col("amount_ranking") === 1)
  highestinvoicetransaction_df.show()

  //finding smallest invoice for same rank
  val windowSpec1 = Window.orderBy("invoice_id")
  val invoiceoutput_df = highestinvoicetransaction_df.withColumn("invoice_ranking" , dense_rank().over(windowSpec1)).filter(col("invoice_ranking") === 1).withColumnRenamed("invoice_id", "invoice")
  invoiceoutput_df.show()

  //joining above dataframe with purchase table to get resultant
  val merged2_df = merged_df.join(invoiceoutput_df, merged_df("invoice_id") === invoiceoutput_df("invoice"), "left").filter(col("invoice").isNotNull)
  val output_df = merged2_df.withColumn("total_price", col("price") * col("quantity")).select("product_id","quantity","total_price")
  output_df.show()


}


object Generate_the_Invoice {
  def main(args: Array[String]) {
    val Generate_the_Invoice = new Generate_the_Invoice()

    def apply(): Generate_the_Invoice = Generate_the_Invoice

  }
}
