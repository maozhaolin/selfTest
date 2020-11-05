package HzTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object ChangeColumn {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf)
      .getOrCreate()

    spark.read.format("jdbc")
      .option("driver","com.mysql.jdbc.Driver")
      .option("url","jdbc:mysql://localhost:3306/fileup")
      .option("dbtable","user")
      .option("user","root")
      .option("password","123456")
      .load()
      .coalesce(1).write.mode(SaveMode.Overwrite).json("hdfs://192.168.216.201:9000/output/user")

    spark.close()
  }
}
