package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_set, explode, size}

class Leetcodify_Friends_Recommendations {
  val spark = SparkSession.builder.master("local").appName("Leetcodify Friends Recommendations").getOrCreate()
  val listen_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/listens.csv")
  val frienship_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/friendship_v2.csv")
  listen_input_df.show()
  frienship_input_df.show()

  //find distinct songs based on grouping day and user on daily basis where song count is >= 3
  listen_input_df.createOrReplaceTempView("listen")
  val daysongcheck_df = spark.sql("select day as song_date, user_id as userid, count(song_id) as song_count from listen group by day, user_id  having count(song_id) >= 3")
  val merge_df = daysongcheck_df.join(listen_input_df, daysongcheck_df("song_date") === listen_input_df("day") && daysongcheck_df("userid") === listen_input_df("user_id"), "left").selectExpr("song_date","userid","song_id","song_count")
  val songidtolist_df = merge_df.groupBy("song_date","userid").agg(collect_set("song_id")).select(col("song_date"),col("userid"), col("collect_set(song_id)").alias("songs_combo"))

  //getting users based on day and song combination
val commonlistenser_df = songidtolist_df.groupBy("song_date","songs_combo").agg(collect_set("userid")).select(col("song_date"),col("songs_combo"), col("collect_set(userid)").alias("users_combo"))

  //filtering records having user_combo more than 1
  val combocheck_df = commonlistenser_df.filter(size(col("users_combo")) >= 3)

  //finding combination for valid users
  val validusercombo_df = combocheck_df.withColumn("user_id", explode(col("users_combo"))).selectExpr("song_date","songs_combo","user_id")

  validusercombo_df.createOrReplaceTempView("users")
  val validusercombo_df_v2 = spark.sql("select a.song_date,a.songs_combo, a.user_id as friend1,b.user_id as friend2 from users a join users b on a.song_date == b.song_date and a.songs_combo == b.songs_combo where a.user_id != b.user_id")


  //not required as per logic.. tech implementation of how to remove reverse duplicates
  validusercombo_df_v2.createOrReplaceTempView("friends")
  val mutualfriends_df = spark.sql("Select t1.*   from friends  t1 inner join friends t2 on t1.friend1 = t2.friend1   and t1.friend2 = t2.friend2 where t1.friend1 < t1.friend2")
  mutualfriends_df.show()

  //finding mutual friends
  val mutualfriendsv2_df = validusercombo_df_v2.join(frienship_input_df, validusercombo_df_v2("friend1") === frienship_input_df("user1_id") && validusercombo_df_v2("friend2") === frienship_input_df("user2_id") || validusercombo_df_v2("friend1") === frienship_input_df("user2_id") && validusercombo_df_v2("friend2") === frienship_input_df("user1_id"), "left_anti").select(col("friend1").alias("user_id"), col("friend2").alias("recommended_id"))
  mutualfriendsv2_df.show()


}



object Leetcodify_Friends_Recommendations {
  def main(args: Array[String]) {
    val Leetcodify_Friends_Recommendations = new Leetcodify_Friends_Recommendations()

    def apply(): Leetcodify_Friends_Recommendations = Leetcodify_Friends_Recommendations

  }
}