package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}

class League_Stats {
  val spark = SparkSession.builder.master("local").appName("League Statistics").getOrCreate()
  val teams_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/teams.csv")
  val matches_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/matches.csv")
  teams_input_df.show()
  matches_input_df.show()

  //calculating total number of matches
  val homematches_df = matches_input_df.select(col("home_team_id")).groupBy("home_team_id").count().select(col("home_team_id"), col("count").alias("home_matches"))
  val awaymatches_df = matches_input_df.select(col("away_team_id")).groupBy("away_team_id").count().select(col("away_team_id"), col("count").alias("away_matches"))
  val matches_df = homematches_df.join(awaymatches_df, homematches_df("home_team_id") === awaymatches_df("away_team_id"), "inner")
  val matchesplayed_df = matches_df.withColumn("matches_played", col("home_matches") + col("away_matches")).select("home_team_id","matches_played")
  matchesplayed_df.show()

  val winningteam_df = matches_input_df.withColumn("winning_team_id", when(col("home_team_goals") > col("away_team_goals"), col("home_team_id")).when(col("home_team_goals") < col("away_team_goals"), col("away_team_id")).when(col("home_team_goals") === col("away_team_goals"), lit("Draw")).otherwise("No Result"))
  val matchwon_df = winningteam_df.where(col("winning_team_id") =!=  "Draw").groupBy("winning_team_id").count().select(col("winning_team_id"), col("count").alias("matches_won"))
  val matchdraw_df = winningteam_df.where(col("winning_team_id") === "Draw")
  winningteam_df.show()
  matchwon_df.show()
  matchdraw_df.show()

  //merging above dataframe with matchesplayed_df to calculate winning and draw score
  val winningstatus_df = matchesplayed_df.join(matchwon_df, matchesplayed_df("home_team_id") === matchwon_df("winning_team_id"), "left").withColumn("match_won", when(col("matches_won").isNull, lit("0") ).otherwise(col("matches_won"))).withColumn("winning_points", col("match_won") * 3)
  val homedrawpoints_df = matchdraw_df.select(col("home_team_id"), col("winning_team_id")).groupBy("home_team_id").count()
  val awaydrawpoints_df = matchdraw_df.select(col("away_team_id"), col("winning_team_id")).groupBy("away_team_id").count()
  val mergeddraw_df = homedrawpoints_df.union(awaydrawpoints_df).groupBy("home_team_id").sum("count").select(col("home_team_id").alias("team_id"), col("sum(count)").alias("matches_draw"))
  val pointstable_df = winningstatus_df.join(mergeddraw_df, winningstatus_df("home_team_id") === mergeddraw_df("team_id"), "left").withColumn("match_draw", when(col("matches_draw").isNull, lit("0") ).otherwise(col("matches_draw"))).withColumn("draw_points", col("match_draw")).withColumn("points", col("winning_points") + col("draw_points")).select("home_team_id","matches_played","match_won","winning_points","match_draw","draw_points","points")
  pointstable_df.show()

  //calculating  goals_for respective teams
  val homegoals_df = matches_input_df.select("home_team_id","home_team_goals").groupBy("home_team_id").sum("home_team_goals").select(col("home_team_id"), col("sum(home_team_goals)").alias("home_goals"))
  val awaygoals_df = matches_input_df.select("away_team_id","away_team_goals").groupBy("away_team_id").sum("away_team_goals").select(col("away_team_id"), col("sum(away_team_goals)").alias("away_goals"))
  val homeaway_df = homegoals_df.join(awaygoals_df, homegoals_df("home_team_id") === awaygoals_df("away_team_id"), "inner").withColumn("goal_for", col("home_goals") + col("away_goals")).select(col("home_team_id"), col("goal_for"))
  homeaway_df.show()

  //calculating goals_against for respective teams
  val home_df = matches_input_df.select(col("home_team_id"), col("away_team_goals")).groupBy("home_team_id").sum("away_team_goals").select(col("home_team_id"), col("sum(away_team_goals)").alias("awaygoal_against"))
  val away_df = matches_input_df.select(col("away_team_id"), col("home_team_goals")).groupBy("away_team_id").sum("home_team_goals").select(col("away_team_id"), col("sum(home_team_goals)").alias("homegoal_against"))
  val homeawayagainst_df = home_df.join(away_df, home_df("home_team_id") === away_df("away_team_id"), "inner").withColumn("goal_against", col("awaygoal_against") + col("homegoal_against")).select(col("home_team_id").alias("home_team_ids"), col("goal_against"))
  val goalforagainst_df = homeaway_df.join(homeawayagainst_df, homeaway_df("home_team_id") === homeawayagainst_df("home_team_ids"), "inner").select("home_team_ids","goal_for", "goal_against")
  goalforagainst_df.show()

  //merging above dataframes for resultant
  val pointstablev2_df = pointstable_df.join(goalforagainst_df, pointstable_df("home_team_id") === goalforagainst_df("home_team_ids"), "inner").select(col("home_team_id"), col("matches_played"), col("points"), col("goal_for"), col("goal_against"))
  val goaldiff_df = pointstablev2_df.withColumn("goal_diff", col("goal_for") - col("goal_against"))
  val output_df = goaldiff_df.join(teams_input_df, goaldiff_df("home_team_id") === teams_input_df("team_id"), "inner").orderBy(col("goal_diff").desc).select("team_name", "matches_played","points","goal_for","goal_against","goal_diff")
  output_df.show()




}


object League_Stats {
  def main(args: Array[String]) {
    val League_Stats = new League_Stats()

    def apply(): League_Stats = League_Stats

  }
}
