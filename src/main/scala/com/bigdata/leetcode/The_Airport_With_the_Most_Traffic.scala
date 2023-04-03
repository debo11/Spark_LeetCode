package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

class The_Airport_With_the_Most_Traffic {
  val spark = SparkSession.builder.master("local").appName("The Airport With the Most Traffic").getOrCreate()
  val flights_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/flights.csv")
  flights_input_df.show()

  //taking aggregating count of flight from respective departure and arrival by distributing the dataset
  val departure_df = flights_input_df.select("departure_airport","flights_count").groupBy("departure_airport").sum("flights_count").select(col("departure_airport"),col("sum(flights_count)").alias("count"))
  val arrival_df = flights_input_df.select("arrival_airport","flights_count").groupBy("arrival_airport").sum("flights_count").select(col("arrival_airport"),col("sum(flights_count)").alias("count"))

  //union above dataframe
  val union_df = departure_df.union(arrival_df)
  union_df.show()

  //taking aggregate sum to resp airports
  val flightcount_df = union_df.groupBy("departure_airport").sum("count").select(col("departure_airport"), col("sum(count)").alias("count"))
  flightcount_df.show()

  //finding most populated airport

  flightcount_df.createOrReplaceTempView("flights")
  val output_df = spark.sql("select a.departure_airport as airport_id from (select departure_airport, count, dense_rank() over ( order by count desc) as rnk from flights) a where a.rnk == 1")
  output_df.show()

}


object The_Airport_With_the_Most_Traffic {
  def main(args: Array[String]) {
    val The_Airport_With_the_Most_Traffic = new The_Airport_With_the_Most_Traffic()

    def apply(): The_Airport_With_the_Most_Traffic = The_Airport_With_the_Most_Traffic

  }
}
