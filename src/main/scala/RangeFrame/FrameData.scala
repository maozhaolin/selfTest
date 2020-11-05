package RangeFrame

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer


object FrameData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FrameData").setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val value: Dataset[String] = spark.read.textFile("data/student/student.txt")

    val value1: RDD[(String, String, String, String)] = value.rdd.map(row => {
      val arr: Array[String] = row.split(" ")
      (arr(0), arr(1), arr(2), arr(3))
    })

    val columns = new ArrayBuffer[String]()
    columns.+=("id")
    columns.+=("name")
    columns.+=("age")
    columns.+=("sex")

    val schema = Seq(columns:_*)

    val frame = spark.createDataFrame(value1).toDF(schema: _*)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val spec = Window.orderBy("age").rangeBetween(Long.MinValue,Long.MaxValue)
    val current = Window.orderBy("age").rangeBetween(Long.MinValue,0)

    val value2: Dataset[Row] = frame
      .select(
      ($"age").alias("age"),
      count("name").over(spec).alias("total"),
      count("name").over(current).alias("current")
    ).select(
      ($"age").alias("age"),
      $"total",
      $"current",
      (($"total" - $"current") / $"total").alias("bili")
    ).where("bili <= 0.125")
        .select(
          min("age").alias("minage")
        )

    value2
        .select(
          $"minage"
        )
      .map(row => {
      val i = row(0).toString.toInt / 2
        ("0<age<=" + i,i + "<age<=" +2*i,"age>" + 2*i)
    }).show()

  }
}
