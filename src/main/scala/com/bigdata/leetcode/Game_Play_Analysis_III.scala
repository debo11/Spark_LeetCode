package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank, lag, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Game_Play_Analysis_III {
  val spark = SparkSession.builder.master("local").appName(" Game Play Analysis 3").getOrCreate()

  //reading inputs
  val activity_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/activity.csv")


  def GamePlay(activity_input_df: DataFrame): DataFrame = {
    info("using lag to get games player total wrt date ")
    val windowSpec = Window.partitionBy("player_id").orderBy(col("event_date")).rowsBetween(Long.MinValue, 0)
    val finaloutput_df = activity_input_df.withColumn("games_played_so_far",  sum("games_played").over(windowSpec)).select("player_id","event_date","games_played_so_far")
    finaloutput_df

  }

  val output_df = GamePlay(activity_input_df)
  output_df.show()


}

object Game_Play_Analysis_III {
  def main(args: Array[String]) {
    val Game_Play_Analysis_III = new Game_Play_Analysis_III()

    def apply(): Game_Play_Analysis_III = Game_Play_Analysis_III

  }
}