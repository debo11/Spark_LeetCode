import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object find_the_quiet_students_in_all_exams {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Find the Quiet Students in All Exams")
      .master("local[*]")
      .getOrCreate()

    val basePath = "C:/Users/DEBASHISH/LeetCode/BD LeetCode/src/resources/"

    // -----------------------------
    //Input tables
    // -----------------------------
    val student_df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(basePath + "student_exam.csv")
      .cache()

    val exam_df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(basePath + "exam.csv")
      .select("exam_id", "student_id", "score")
      .cache()

    // -----------------------------
    // Single window spec
    // -----------------------------
    val wExam = Window.partitionBy("exam_id")
    val wAsc  = Window.partitionBy("exam_id").orderBy(col("score").asc)
    val wDesc = Window.partitionBy("exam_id").orderBy(col("score").desc)

    // -----------------------------
    // Window function to eliminate top and lower students
    // -----------------------------
    val enriched = exam_df
      .withColumn("exam_cnt", count("*").over(wExam))
      .withColumn("rmin", dense_rank().over(wAsc))
      .withColumn("rmax", dense_rank().over(wDesc))
      .cache()

    // -----------------------------
    // Students to exclude
    // -----------------------------
    val excludedStudents = enriched
      .filter(
        col("exam_cnt") < 3 ||
          col("rmin") === 1 ||
          col("rmax") === 1
      )
      .select("student_id")
      .distinct()
      .cache()

    // -----------------------------
    // Mid scores only
    // -----------------------------
    val midScores = enriched
      .filter(col("exam_cnt") >= 3 && col("rmin") > 1 && col("rmax") > 1)
      .join(broadcast(excludedStudents), Seq("student_id"), "left_anti")
      .select("student_id")
      .cache()

    // -----------------------------
    // Final quiet students
    // -----------------------------
    val finalOutput = student_df
      .join(broadcast(midScores.distinct()), Seq("student_id"), "inner")

    finalOutput.show(false)

  }
}
