package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class The_Category_of_Each_Member_in_the_Store {
  val spark = SparkSession.builder.master("local").appName("Find the Start and End Number of Continuous Ranges").getOrCreate()

  //reading inputs
  val members_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/members.csv")
  val visits_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/visits_v2.csv")
  val purchases_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/purchases.csv")

  def StoreCategory(members_input_df: DataFrame,visits_input_df:DataFrame,purchases_input_df:DataFrame): DataFrame = {
    info("fining number of visits")
    val customervisit_df = members_input_df.join(visits_input_df, members_input_df("member_id") === visits_input_df("member_id"), "left")

    info(" Customer who visited and purchased")
    val customerpurchase_df = customervisit_df.filter(col("visit_id").isNotNull).select("name").groupBy("name").count().select(col("name"), col("count").alias("visit_count"))
    val customernonpurchase_df = customervisit_df.filter(col("visit_id").isNull).select("name").withColumn("visit_count", lit(0))
    val finalcutomervist_df = customerpurchase_df.union(customernonpurchase_df).withColumnRenamed("name","cust_name")
    finalcutomervist_df

    info("merging purchase with visit to find out customer who purchased")
    val purchasevisit_df = purchases_input_df.join(visits_input_df, purchases_input_df("visit_id") === visits_input_df("visit_id"), "left").select("member_id").groupBy("member_id").count().select(col("member_id"), col("count").alias("purchase_count"))

    info("merging customervisit and finalcustomervisit for resultant calculation")
    val finalpurchasevisit_df = finalcutomervist_df.join(members_input_df, finalcutomervist_df("cust_name") === members_input_df("name"), "left").select("cust_name","visit_count","member_id").withColumnRenamed("member_id","mem_id")
    val finalmerge_df = finalpurchasevisit_df.join(purchasevisit_df, finalpurchasevisit_df("mem_id") === purchasevisit_df("member_id"), "left").select("cust_name","mem_id","visit_count","purchase_count")
    val categorycalc_df = finalmerge_df.withColumn("category_value", (lit(100) * col("purchase_count")) / (col("visit_count")))
    val categorydistribution_df = categorycalc_df.withColumn("category", when(col("visit_count") === 0 && col("purchase_count").isNull, lit("Bronze")).when(col("visit_count").isNotNull && col("purchase_count").isNull, lit("Silver")).when(col("category_value") >= 80, lit("Diamond")).when(col("category_value").between(50,80), lit("Gold")).when(col("category_value") < 50, lit("Silver")).otherwise("NA")).select("mem_id","cust_name","category").orderBy("mem_id")
    categorydistribution_df

  }



  val output_df = StoreCategory(members_input_df,visits_input_df,purchases_input_df)
  output_df.show()


}


object The_Category_of_Each_Member_in_the_Store {
  def main(args: Array[String]) {
    val The_Category_of_Each_Member_in_the_Store = new The_Category_of_Each_Member_in_the_Store()

    def apply(): The_Category_of_Each_Member_in_the_Store = The_Category_of_Each_Member_in_the_Store

  }
}


