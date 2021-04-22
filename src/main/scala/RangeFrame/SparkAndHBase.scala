package RangeFrame

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkAndHBase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkAndHBase").setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM,"Hadoop001")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT,"2181")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,"test")

    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val frame = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.182.1:3306/selftest")
      .option("dbtable", "cityinfo")
      .option("user", "root")
      .option("password", "123456")
      .load()

    val dataRDD = frame.rdd
    val columns = frame.columns
    val data = dataRDD.map(item =>{
      val arr = item.toString()
        .replace("[","")
        .replace("]","")
        .split(",")
      val key = arr(0)
      val put = new Put(Bytes.toBytes(key))
      for(e <- 0 until arr.length){
        put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes(columns(e)),Bytes.toBytes(arr(e)))
      }
      (new ImmutableBytesWritable(),put)
    })

    data.foreach(println(_))

    data.saveAsHadoopDataset(jobConf)

    spark.close()
  }
}
