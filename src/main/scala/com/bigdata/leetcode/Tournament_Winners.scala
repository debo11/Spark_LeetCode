package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Tournament_Winners {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Tournament Winners ")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // =========================
    // Read inputs ()
    // =========================
    val basePath = "C:/Users/DEBASHISH/LeetCode/BD LeetCode/src/resources/"

    val playersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(basePath + "players.csv")
      .select("player_id", "group_id")
      .cache()

    val matchesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(basePath + "match.csv")
      .select("first_player", "second_player", "first_score", "second_score")
      .cache()

    // ==============================================
    // Transform matches into single normalized format
    // (avoids union + multiple groupBy)
    // ==============================================
    val normalizedScoresDF = matchesDF
      .select(col("first_player").alias("player"), col("first_score").alias("score"))
      .unionByName(
        matchesDF.select(col("second_player").alias("player"), col("second_score").alias("score"))
      )

    // ==============================================
    // Single aggregation instead of 2 + union + agg
    // ==============================================
    val totalScorePerPlayerDF = normalizedScoresDF
      .groupBy("player")
      .agg(sum("score").alias("total_score"))

    // =========================
    // Join with players table
    // =========================
    val joinedDF = playersDF
      .join(broadcast(totalScorePerPlayerDF), playersDF("player_id") === totalScorePerPlayerDF("player"), "left")
      .drop("player")

    // =========================
    // Ranking logic
    // =========================
    val scoreWindow = Window.partitionBy("group_id").orderBy(desc("total_score"), asc("player_id"))

    val winnersDF = joinedDF
      .withColumn("rank", row_number().over(scoreWindow))
      .filter(col("rank") === 1)
      .select("group_id", "player_id")

    // =========================
    // Output
    // =========================
    winnersDF.show(false)
  }
}
