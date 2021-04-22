package RangeFrame

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparkTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_test").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()


    val frame = spark.read.csv("C:\\Users\\86186\\Desktop\\aa.csv")

    import org.apache.spark.sql.functions._
    val frame1 = frame.withColumn("roundNum", round(rand() * (51 - 1) + 1, 0))


    spark.close()
  }
}
