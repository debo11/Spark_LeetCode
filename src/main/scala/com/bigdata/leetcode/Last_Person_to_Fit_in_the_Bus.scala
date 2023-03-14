package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, when,sum}

class Last_Person_to_Fit_in_the_Bus {
  val spark = SparkSession.builder.master("local").appName("Last Person to Fit in the Bus").getOrCreate()
  val queue_input_df = spark.read.format("csv").option("header", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/queue.csv").orderBy("turn")
  queue_input_df.show()

  //finding last person with weight limit of 1000kg

  val windowSpec = Window.orderBy("turn")
  val weightlimit_df = queue_input_df.withColumn("sum", sum("weight").over(windowSpec))
  val finalcheck_df = weightlimit_df.withColumn("Check", when(col("sum") === 1000, lit("Last Person")).when(col("sum") < 1000, lit("Can Sit")).otherwise("No Place to Sit"))
  val finalOutput_df = finalcheck_df.filter(col("Check") === "Last Person").select("person_name")
  weightlimit_df.show()
  finalcheck_df.show()
  finalOutput_df.show()
}

object Last_Person_to_Fit_in_the_Bus {
  def main(args: Array[String]) {
    val Last_Person_to_Fit_in_the_Bus = new Last_Person_to_Fit_in_the_Bus()

    def apply(): Last_Person_to_Fit_in_the_Bus = Last_Person_to_Fit_in_the_Bus

  }
}