package RangeFrame

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkUtils {


  def getSpark(redisHosts:String,redisPort:String,redisDB:String):SparkSession={
    val conf = new SparkConf().setAppName("Spark-Redis").setMaster("local[*]")
      .set("spark.redis.host",redisHosts)
      .set("spark.redis.port", redisPort)
      //      .set("spark.redis.auth","123456") //指定redis密码
      .set("spark.redis.db",redisDB)
    val spark = SparkSession.builder().config(conf)
      .getOrCreate()
    spark
  }

  def redisRead(spark:SparkSession,tableName:String,keyName:String):DataFrame = {
    val frame = spark.read
      .format("org.apache.spark.sql.redis")
      .option("table",tableName )
      .option("key.column", keyName)
      .load()
    frame
  }

  def redisWrite(frame:DataFrame,tableName:String,keyName:String) = {
    frame.write
      .format("org.apache.spark.sql.redis")
      .option("table", tableName)
      .option("key.column", keyName)
      .mode(SaveMode.Overwrite)
      .save()
  }
}
