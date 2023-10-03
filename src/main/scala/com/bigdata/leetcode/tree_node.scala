package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class tree_node {


  val spark = SparkSession.builder.master("local").appName("Tree Node").getOrCreate()

  //reading inputs
  val tree_titles_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/tree.csv")

  def TreeNode(tree_titles_input_df: DataFrame): DataFrame = {
    info("logic for root, inner,Leaf check")
    val parentid = tree_titles_input_df.select("p_id").filter(col("p_id") =!= "null").collect().map(_(0)).toList
    val node_identifier = tree_titles_input_df.withColumn("type", when(col("p_id").isNull || col("p_id") === "null", lit("Root")).when(col("id").isin(parentid:_*),lit("Inner")).otherwise("Leaf"))
    node_identifier
  }

  val output_df = TreeNode(tree_titles_input_df)
  output_df.show()

}


object tree_node {
  def main(args: Array[String]) {
    val tree_node = new tree_node()

    def apply(): tree_node = tree_node

  }
}