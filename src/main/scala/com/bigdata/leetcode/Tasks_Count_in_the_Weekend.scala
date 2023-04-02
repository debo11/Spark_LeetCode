package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, lit, when}

class Tasks_Count_in_the_Weekend {
  val spark = SparkSession.builder.master("local").appName("Tasks Count in the Weekend").getOrCreate()
  val tasks_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/tasks.csv")
  tasks_input_df.show()

//calculating days from given date input
  val day_df = tasks_input_df.withColumn("day", date_format(col("submit_date"), "E"))
  day_df.show()

  //differentiating weekdays and weekends
  val weekdayend_df = day_df.withColumn("week_check", when(col("day") === "Sat" || col("day") === "Sun", lit("Weekend")).otherwise("Weekdays"))
  weekdayend_df.show()

  //aggregating based on week_check
  val output_df = weekdayend_df.groupBy("week_check").count()
  output_df.show()

}



object Tasks_Count_in_the_Weekend {
  def main(args: Array[String]) {
    val Tasks_Count_in_the_Weekend = new Tasks_Count_in_the_Weekend()

    def apply(): Tasks_Count_in_the_Weekend = Tasks_Count_in_the_Weekend

  }
}
