package com.bigdata.leetcode

import org.apache.ivy.util.Message.info
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Number_of_Trusted_Contacts_of_a_Customer {
  val spark = SparkSession.builder.master("local").appName("Number of Trusted Contacts of a Customer").getOrCreate()

  //reading inputs
  val customers_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/Customers.csv")
  val contacts_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/Contacts.csv")
  val invoices_input_df = spark.read.format("csv").option("header", "true").option("InferSchema", "true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/invoices.csv")
  customers_input_df.show()
  contacts_input_df.show()
  invoices_input_df.show()


  def CustomerContact (customers_input_df:DataFrame,contacts_input_df:DataFrame,invoices_input_df:DataFrame):DataFrame={
    info("merging invoice and customer table to get first 3 required columns")
    val invoicecustomer_df = invoices_input_df.join(customers_input_df, invoices_input_df("user_id") === customers_input_df("customer_id"), "inner").select("invoice_id", "customer_name", "price","user_id").orderBy("invoice_id")

    info("finding customer contact")
    val contact_count_df = contacts_input_df.select("user_id").groupBy("user_id").count().select(col("user_id"), col("count").alias("contacts_cnt"))
    val inv_cus_df = contact_count_df.join(customers_input_df,contact_count_df("user_id") === customers_input_df("customer_id"), "inner").withColumnRenamed("user_id","users_id")

    info("finding trusted customer count")
    val trusted_cust_df = contacts_input_df.join(inv_cus_df, contacts_input_df("user_id") === inv_cus_df("users_id"),"left").select("users_id","contact_name","contacts_cnt","customer_name","contact_email")
    val validemail_df = trusted_cust_df.join(customers_input_df, trusted_cust_df("contact_email") === customers_input_df("email"), "left")
    val emailcheck_df = validemail_df.withColumn("email_check", when(col("email").isNotNull, lit(1)).otherwise("0")).select(col("users_id"),col("email_check").cast("numeric")).groupBy("users_id").sum("email_check").select(col("users_id").alias("users_ids"),col("sum(email_check)").alias("trusted_contacts_cnt"))

    info("merging above dataframe to get resultant")
   val contact_df = emailcheck_df.join(trusted_cust_df, emailcheck_df("users_ids") === trusted_cust_df("users_id") , "inner").select("users_id","contacts_cnt","trusted_contacts_cnt").distinct()
    val contactcustomer_df = invoicecustomer_df.join(contact_df, invoicecustomer_df("user_id") === contact_df("users_id"), "inner").orderBy("invoice_id").select("invoice_id","customer_name","price","contacts_cnt","trusted_contacts_cnt")

    info("finding  non trusted customers ")
    val nontrusted_df = invoices_input_df.join(contact_count_df, contact_count_df("user_id") === invoices_input_df("user_id"), "left_anti")
    val nontrustedcustomer_df = nontrusted_df.join(customers_input_df, nontrusted_df("user_id") === customers_input_df("customer_id"), "inner").select("invoice_id","customer_name","price").withColumn("contacts_cnt", lit(0)).withColumn("trusted_contacts_cnt", lit(0))

    info("merging trusted and non trusted customers")
    val output_df = contactcustomer_df.union(nontrustedcustomer_df).orderBy("invoice_id")
    output_df




  }

  val output_df = CustomerContact(customers_input_df, contacts_input_df,invoices_input_df)
  output_df.show()

}


object Number_of_Trusted_Contacts_of_a_Customer {
  def main(args: Array[String]) {
    val Number_of_Trusted_Contacts_of_a_Customer = new Number_of_Trusted_Contacts_of_a_Customer()

    def apply(): Number_of_Trusted_Contacts_of_a_Customer = Number_of_Trusted_Contacts_of_a_Customer

  }
}