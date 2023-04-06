package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank}


  class The_Change_in_Global_Rankings {
    val spark = SparkSession.builder.master("local").appName("The Change in Global Rankings").getOrCreate()
    val teamPoints_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/teampoints.csv")
    val pointsChange_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/pointschange.csv")
    teamPoints_input_df.show()
    pointsChange_input_df.show()

    //finding current position based on pints table
    teamPoints_input_df.createOrReplaceTempView("points")
    val currentposition_df = spark.sql("select a.team_id as team_ids,a.name as team_name,a.points as current_points,a.rnk as current_rank from (select team_id,name,points, dense_rank() over (order by points desc) as rnk from points) a")
    currentposition_df.show()

    //merging pointschange to get change in points to respective teams
    val merged_df = currentposition_df.join(pointsChange_input_df, currentposition_df("team_ids") === pointsChange_input_df("team_id"), "inner").selectExpr("team_id","team_name","current_points","current_rank","points_change")
    merged_df.show()

    //calculating new points
    val newpoints_df = merged_df.withColumn("updated_points", col("current_points") + col("points_change"))
    newpoints_df.show()

    // calculating new ranking based on updated points
    newpoints_df.createOrReplaceTempView("points")
    val updatedrank_df = spark.sql("select a.team_id,a.team_name,a.current_points,a.points_change,a.current_rank,a.updated_points,a.rnk as updated_rank from (select team_id,team_name,current_points,points_change,current_rank,updated_points, dense_rank() over (order by updated_points desc, team_name asc) as rnk from points) a")
    updatedrank_df.show()

    //calculating final ranking difference

    val output_df = updatedrank_df.withColumn("rank_diff", col("current_rank") - col("updated_rank")).selectExpr("team_id","team_name","rank_diff")
    output_df.show()

}


  object The_Change_in_Global_Rankings {
    def main(args: Array[String]) {
      val The_Change_in_Global_Rankings = new The_Change_in_Global_Rankings()

      def apply(): The_Change_in_Global_Rankings = The_Change_in_Global_Rankings

    }
  }
