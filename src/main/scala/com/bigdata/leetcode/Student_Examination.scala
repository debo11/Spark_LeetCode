package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class Student_Examination {

  val spark = SparkSession.builder.master("local").appName("Student and Examinations").getOrCreate()
  val students_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/students_v2.csv")
  val subject_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/subjects.csv")
  val examination_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/examinations.csv")


  def StudentExamination(students_input_df: DataFrame, subject_input_df: DataFrame, examination_input_df:DataFrame): DataFrame = {
    info("finding valid sbjects")
    val varename_df = subject_input_df.withColumnRenamed("subject_name","sub_name")
    val varename2_df = students_input_df.withColumnRenamed("student_id","stu_id")
    val validsubject_df = examination_input_df.join(varename_df, examination_input_df("subject_name") === varename_df("sub_name"), "right").select("student_id","sub_name")

    info("to check total exams taken for each students")
    val examstaken_df = validsubject_df.groupBy("sub_name","student_id").count().select(col("sub_name"),col("student_id"),col("count").alias("total_count"))

    info("merging above dataframe with students")
    val merged_df = examstaken_df.join(varename2_df, examstaken_df("student_id") === varename2_df("stu_id"),"inner").select(col("student_id"),col("student_name"),col("sub_name"),col("total_count").alias("attended_exams")).orderBy("student_id","sub_name")
    merged_df
  }


  val output_df = StudentExamination(students_input_df, subject_input_df, examination_input_df)
  output_df.show()

}


object Student_Examination {
  def main(args: Array[String]) {
    val Student_Examination = new Student_Examination()

    def apply(): Student_Examination = Student_Examination

  }
}