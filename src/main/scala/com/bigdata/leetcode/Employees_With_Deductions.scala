package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, log, regexp_replace, round, substring, to_timestamp, when}

class Employees_With_Deductions {
  val spark = SparkSession.builder.master("local").appName("Employees With Deductions").getOrCreate()
  val employees_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/employees.csv")
  val logs_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/logs.csv")
  employees_input_df.show()
  logs_input_df.show()

  //calculating login based on monthly basis
  val logintime_df = logs_input_df.withColumn("in_period", substring(col("in_time"),1,7)).withColumn("out_period", substring(col("out_time"),1,7))
  logintime_df.show()

  //finding time differnce
  val timediff_df = logintime_df.withColumn("time_in_sec", col("out_time").cast("Long") -col("in_time").cast("Long")).withColumn("DiffInHours",round(col("time_in_sec")/3600,2))
    timediff_df.show()


  val aggregatedtime_df = timediff_df.groupBy("employee_id","in_period","out_period").sum("DiffInHours").select(col("employee_id").alias("emp_id"),col("in_period"),col("out_period"),col("sum(DiffInHours)").alias("total_time_logged"))
  aggregatedtime_df.show()

  //mergin employee table with above dataframe to find resultant

  val merged_df = employees_input_df.join(aggregatedtime_df, aggregatedtime_df("emp_id") === employees_input_df("employee_id"), "left_outer")
  val logincheck_df = merged_df.withColumn("timing_check", when(col("total_time_logged") >= col("needed_hours"), lit("No Deduction")).otherwise("Deduction"))
  val output_df = logincheck_df.filter(col("timing_check") === "Deduction").select("employee_id")

  output_df.show()



}


object Employees_With_Deductions {
  def main(args: Array[String]) {
    val Employees_With_Deductions = new Employees_With_Deductions()

    def apply(): Employees_With_Deductions = Employees_With_Deductions

  }
}