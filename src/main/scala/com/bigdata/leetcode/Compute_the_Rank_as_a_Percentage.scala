package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Compute_the_Rank_as_a_Percentage {
  val spark = SparkSession.builder.master("local").appName(" Compute the Rank as a Percentage").getOrCreate()

  //reading inputs
  val students_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/students.csv")

  def RankPercentage (students_input_df:DataFrame):DataFrame={
    info("using dense rank to rank students")
    val windowSpec = Window.partitionBy("department_id").orderBy(col("mark").desc)
    val ranking_df = students_input_df.withColumn("rank", dense_rank().over(windowSpec))
    ranking_df

    info("find count of student per department")
    val deparstucount_df = ranking_df.select("department_id").groupBy("department_id").count().select(col("department_id"), col("count").alias("student_department_count")).withColumnRenamed("department_id","dept_id")
    ranking_df

    info("merging aboce dataframe to calculate further resultant")
    val merged_df = ranking_df.join(deparstucount_df, ranking_df("department_id") === deparstucount_df("dept_id"), "left").select("student_id","department_id","mark","rank","student_department_count")
    merged_df

    info("calculating percentage")
    val percentage_df = merged_df.withColumn("percentage", (col("rank") - 1) * 100 / (col("student_department_count") - 1)).select("student_id","department_id","percentage")
    percentage_df

  }

  val output_df = RankPercentage(students_input_df)
  output_df.show()

}



object Compute_the_Rank_as_a_Percentage {
  def main(args: Array[String]) {
    val Compute_the_Rank_as_a_Percentage = new Compute_the_Rank_as_a_Percentage()

    def apply(): Compute_the_Rank_as_a_Percentage = Compute_the_Rank_as_a_Percentage

  }
}