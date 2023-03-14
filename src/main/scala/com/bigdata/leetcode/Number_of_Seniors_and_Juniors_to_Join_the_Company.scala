package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, dense_rank, lag, lead, length, lit, max, monotonically_increasing_id, sum, when}

object Number_of_Seniors_and_Juniors_to_Join_the_Company{

def main(args: Array[String]) {

  val spark = SparkSession.builder.master("local").appName("The Number of Seniors and Juniors to Join the Company").getOrCreate()
  val candidiates_input_df = spark.read.format("csv").option("header", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/candidates.csv")
  candidiates_input_df.show()

  //using window  func to get corresponding sum of salary wrt expeience
  val windowSpec  = Window.partitionBy("experience").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
  val windowSpec2  = Window.partitionBy("experience").orderBy("id")
  val salarydis_df = candidiates_input_df.withColumn("sum",  sum("salary").over(windowSpec))
  val df_id = salarydis_df.withColumn("id", monotonically_increasing_id()+1)
  val expsalary_df = df_id.withColumn("rank",  dense_rank().over(windowSpec2))
    salarydis_df.show()
    expsalary_df.show()

  //filtering out seniors records and count and remaining budget for juniors
  val selectedseniors_df = expsalary_df.filter(col("experience") === "Senior").withColumn("status", when(col("sum") <= 70000, lit("valid")).otherwise(lit("invalid")))
  val validseniorscount_df = selectedseniors_df.filter(col("status") === "valid").groupBy("experience").max("sum").select(col("experience").alias("experience_status"), col("max(sum)").alias("spend_amount")).withColumn("remaining_budget", lit(70000) - col("spend_amount"))
  val seniorcount = selectedseniors_df.filter(col("status") === "valid").groupBy("experience").max("rank").alias("accepted_candidates").select(col("experience"), col("max(rank)").alias("accepted_candidate"))
  val senior_df = validseniorscount_df.join(seniorcount, validseniorscount_df("experience_status") === seniorcount("experience"), "inner").selectExpr("experience","accepted_candidate")
  selectedseniors_df.show()
  validseniorscount_df.show()
  seniorcount.show()
  senior_df.show()

  validseniorscount_df.createOrReplaceTempView("fb")
  val remaingbudget_df = spark.sql("select remaining_budget from fb").first().getDouble(0)


  //for juniors selection with remaining budget
  val selectedjuniors_df = expsalary_df.filter(col("experience") === "Junior").withColumn("status", when(col("sum") <= remaingbudget_df, lit("valid")).otherwise(lit("invalid")))
    selectedjuniors_df.show()
  val junior_df = selectedjuniors_df.filter(col("status") === "valid").groupBy("experience").max("rank").select(col("experience"), col("max(rank)").alias("accepted_candidate"))
  junior_df.show()

  //merging both senior and junior df for resultant
  val output_df = senior_df.union(junior_df)
  output_df.show()


}
}
