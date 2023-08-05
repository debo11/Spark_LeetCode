package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, concat, lit}

class Musical_Event {

  val spark = SparkSession.builder.master("local").appName("Musical Event").getOrCreate()

  //reading inputs
  val musicians_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/musician.csv")

  def Musicians(musicians_input_df: DataFrame): DataFrame = {
    info("finding musicians more than 1 instruments")
    val multiple_instrument_df = musicians_input_df.select("MusID").groupBy("MusID").count().select(col("MusID"), col("count").alias("instrument_count")).filter(col("instrument_count") >1 )

    info("filtering musicians where instruments are flute")
    val flute_musicians_df = musicians_input_df.filter(col("Instrument") === "Flute" ).withColumnRenamed("MusID", "music_id")

    info("joining above dataframe tp get musicians with more than 1 instrument and one of them being flute")
    val merged_df = multiple_instrument_df.join(flute_musicians_df, multiple_instrument_df("MusID") === flute_musicians_df("music_id")).select(col("music_id"),col("LastName").alias("LastN"),col("FirstN").alias("FirstName"))
    val output_df = merged_df.join(musicians_input_df, merged_df("music_id") === musicians_input_df("MusID"), "left").select("music_id","LastName","FirstN","Instrument").groupBy("music_id","LastName","FirstN").agg(collect_list("Instrument")).select(col("music_id"),col("LastName"),col("FirstN"),col("collect_list(Instrument)").alias("Instrument_list"))
    output_df


  }


val output_df = Musicians(musicians_input_df)
output_df.show()


}

object Musical_Event {
  def main(args: Array[String]) {
    val Musical_Event = new Musical_Event()

    def apply(): Musical_Event = Musical_Event

  }
}