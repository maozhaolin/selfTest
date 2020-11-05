package player


import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object bkjb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark_kafka").master("local[*]").getOrCreate()
    val batchDuration = Seconds(5) //时间单位为秒
    val streamContext = new StreamingContext(spark.sparkContext, batchDuration)
    streamContext.checkpoint("/Users/eric/SparkWorkspace/checkpoint")
    val topics = Array("test").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "Hadoop001:9092,Hadoop002:9092,Hadoop003:9092")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamContext, kafkaParams, topics)
    stream.foreachRDD(rdd => {
      rdd.foreach(line => {
        println("key=" + line._1 + "  value=" + line._2)
      })
    })
    streamContext.start()  //spark stream系统启动
    streamContext.awaitTermination() //
  }
}
