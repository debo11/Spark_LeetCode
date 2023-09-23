package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Cookbook_Recipes {


  val spark = SparkSession.builder.master("local").appName("Cooking Recipes").getOrCreate()

  //reading inputs
  val cookbook_titles_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/cookbook_titles.csv")

  def CookingRecipe(cookbook_titles_input_df: DataFrame): DataFrame = {
    info("creating 2 columns with odd and even numbers")
   val evenpage_df = cookbook_titles_input_df.withColumn("even_no", monotonically_increasing_id() * 2)
    val oddpage_df = evenpage_df.withColumn("odd_no", (monotonically_increasing_id() * 2) + 1).select("even_no","odd_no")

    info("joining above dataframes with input dataframe")
    val lefttitle_df = oddpage_df.join(cookbook_titles_input_df, oddpage_df("even_no") === cookbook_titles_input_df("page_number"), "left").withColumnRenamed("title","left_title").withColumnRenamed("page_number","pg_no")
    val righttitle_df = lefttitle_df.join(cookbook_titles_input_df, lefttitle_df("odd_no") === cookbook_titles_input_df("page_number"),"left").withColumnRenamed("title","right_title").withColumnRenamed("even_no","left_page_number").select("left_page_number","left_title","right_title").na.fill("")
    righttitle_df

  }

  val output_df = CookingRecipe(cookbook_titles_input_df)
  output_df.show()
}

object Cookbook_Recipes {
  def main(args: Array[String]) {
    val Cookbook_Recipes = new Cookbook_Recipes()

    def apply(): Cookbook_Recipes = Cookbook_Recipes

  }
}
