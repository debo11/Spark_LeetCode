package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{bround, col, datediff}

class risky_projects {


  val spark = SparkSession.builder.master("local").appName("Risky Projects").getOrCreate()

  //reading inputs
  val linkedin_projects_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/linkedin_projects.csv")
  val linkedin_emp_projects_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/linkedin_emp_projects.csv")
  val linkedin_employees_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/linkedin_employees.csv")

  def PercentageDiff(linkedin_projects_input_df: DataFrame, linkedin_emp_projects_input_df: DataFrame, linkedin_employees_input_df: DataFrame): DataFrame = {

    info("to find total months for a project")
    val monthsallocation_df = linkedin_projects_input_df.withColumn("total_project_days", datediff(col("end_date") , col("start_date")));

    info("finding total employees for each projects")
    val employeeperproject_df = linkedin_emp_projects_input_df.groupBy("project_id").count().select(col("project_id"), col("count").alias("employee_count")).orderBy("project_id")

    info("joining linkedin_emp_projects_input_df and linkedin_employees_input_df")
    val employeeprojcountDF = linkedin_emp_projects_input_df.join(linkedin_employees_input_df, linkedin_emp_projects_input_df("emp_id") === linkedin_employees_input_df("id"), "left")
      .groupBy("project_id").sum("salary").select(col("project_id"), col("sum(salary)").divide(365).alias("employee_total_salary_per_day"));

    info("joining monthsallocation_df with  employeeprojcountDF to calcualte employee total expense")
    val finalOutputDF = monthsallocation_df.join(employeeprojcountDF, monthsallocation_df("id") === employeeprojcountDF("project_id"), "left")
      .withColumn("prorated_employee_expense", bround(col("total_project_days").multiply(col("employee_total_salary_per_day"))))
    .select("title","budget","prorated_employee_expense");

    finalOutputDF

}


val output_df = PercentageDiff(linkedin_projects_input_df,linkedin_emp_projects_input_df,linkedin_employees_input_df)
output_df.show()
}

object risky_projects {
  def main(args: Array[String]) {
    val risky_projects = new risky_projects()

    def apply(): risky_projects = risky_projects

  }
}
