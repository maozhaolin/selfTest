package player

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object bkjb {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("lla").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()


    spark.close()
  }
}
