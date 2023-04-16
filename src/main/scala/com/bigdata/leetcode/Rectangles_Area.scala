package com.bigdata.leetcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, when}

class Rectangles_Area {
  val spark = SparkSession.builder.master("local").appName("Rectangles Area").getOrCreate()
  val points_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/points.csv")
  points_input_df.show()

  //finding all unique combination of ids
  points_input_df.createOrReplaceTempView("points")
  val idcombination_df = spark.sql("select a.id as  coordinate_1,b.id as coordinate_2 from points a join points b where a.id <> b.id ")
  idcombination_df.createOrReplaceTempView("points")
  val uniqueid_df = spark.sql("Select t1.*   from points  t1 inner join points t2 on t1.coordinate_1 = t2.coordinate_1   and t1.coordinate_2 = t2.coordinate_2 where t1.coordinate_1 < t1.coordinate_2")
  idcombination_df.show()
  uniqueid_df.show()

  //joining above dataframe with points table to get respective (x1,y1) (x2,y2)
  val coordinate1_df = uniqueid_df.join(points_input_df, uniqueid_df("coordinate_1") === points_input_df("id"), "inner").select(col("coordinate_1"), col("coordinate_2"),col("x_value").alias("coordinate1_x"), col("y_value").alias("coordinate1_y"))
  val coordinate2_df = coordinate1_df.join(points_input_df, coordinate1_df("coordinate_2") === points_input_df("id"), "inner").select(col("coordinate_1"), col("coordinate_2"),col("coordinate1_x"), col("coordinate1_y"),col("x_value").alias("coordinate2_x"),col("y_value").alias("coordinate2_y"))
  coordinate1_df.show()
  coordinate2_df.show()

  //calculating area based on coordinates
  val area_df = coordinate2_df.withColumn("area", (col("coordinate2_x") - col("coordinate1_x")) * (col("coordinate2_y") - col("coordinate1_y"))).withColumn("area_check", when(col("area") < 0, col("area")* -1).otherwise(col("area")))
  area_df.show()

  //filtering out area grater than 0
  val output_df = area_df.select(col("coordinate_1").alias("p1"), col("coordinate_2").alias("p2"), col("area_check").alias("area")).filter(col("area") > 0).orderBy(desc("area"))
  output_df.show()


}

object Rectangles_Area {
  def main(args: Array[String]) {
    val Rectangles_Area = new Rectangles_Area()

    def apply(): Rectangles_Area = Rectangles_Area

  }
}