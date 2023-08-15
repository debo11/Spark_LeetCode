package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, dense_rank, lit, when}

class Status_of_Flight_Tickets {


  val spark = SparkSession.builder.master("local").appName("Status of Flight Tickets").getOrCreate()

  //reading inputs
  val flights_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/flights_v2.csv")
  val passengers_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/passengers.csv").withColumnRenamed("flight_id","flights_id")


  def FlightStatus(flights_input_df: DataFrame,passengers_input_df:DataFrame): DataFrame = {
    info("merging above dataframes")
    val merged_df = passengers_input_df.join(flights_input_df, passengers_input_df("flights_id") === flights_input_df("flight_id"), "inner").orderBy("booking_time").select("passenger_id","flight_id","booking_time","capacity")
    val windowSpec = Window.partitionBy("flight_id").orderBy("booking_time")
    val passenger_ranking_df = merged_df.withColumn("ranking", dense_rank().over(windowSpec))

    info("finding out confirmed and waitinglist passengers")
    val status_df = passenger_ranking_df.withColumn("status", when(col("ranking") <= col("capacity"), lit("Confimed")).otherwise("Waitlist"))
    val output_df = status_df.orderBy("passenger_id").select("passenger_id","status")
    passenger_ranking_df


  }

  val output_df = FlightStatus(flights_input_df,passengers_input_df)
  output_df.show()

}


object Status_of_Flight_Tickets {
  def main(args: Array[String]) {
    val Status_of_Flight_Tickets = new Status_of_Flight_Tickets()

    def apply(): Status_of_Flight_Tickets = Status_of_Flight_Tickets

  }
}