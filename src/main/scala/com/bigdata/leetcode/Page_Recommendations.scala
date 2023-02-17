package com.bigdata.leetcode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Page_Recommendations {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local").appName("Page Recommendations").getOrCreate()
    val df_friends_input = spark.read.format("csv").option("header", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/friendship.csv")
    val df_likes_input = spark.read.format("csv").option("header", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/likes.csv")
    df_friends_input.show()
    df_likes_input.show()

    //getting friends for user_id =1
    val user1_df = df_friends_input.filter(col("user1_id") === 1).select(col("user1_id"), col("user2_id").alias("user_friends"))
    user1_df.show()
    val user2_df = df_friends_input.filter(col("user2_id") === 1).select(col("user2_id"), col("user1_id").alias("user_friends"))
    user2_df.show()
    val mergeuser_df = user1_df.union(user2_df)
    mergeuser_df.show()

    //finding post likes by user_id 1 friends
    val merge_df = df_likes_input.join(mergeuser_df, mergeuser_df("user_friends") === df_likes_input("user_id") || mergeuser_df("user1_id") === df_likes_input("user_id"), "inner").selectExpr("user_id","page_id").distinct().orderBy("user_id")
    merge_df.show()

    //finding pagesuser_id=1 liked
    val userlikes_df = df_likes_input.filter(col("user_id") === 1)
    userlikes_df.show()

    //finding recommended pages for user_id=1
    val output_df = merge_df.join(userlikes_df, merge_df("page_id") === userlikes_df("page_id"), "left_anti").select(col("page_id").alias("recommended_page")).distinct().orderBy("page_id")
      output_df.show()

  }
}