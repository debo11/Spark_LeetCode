import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.master("local").appName("Investments in 2016").getOrCreate()
    val df_input = spark.read.format("csv").option("header","true").load("C:\\Users\\DEBASHISH\\LeetCode\\BD LeetCode\\src\\resources/insurance.csv")
    df_input.show()
  }
}