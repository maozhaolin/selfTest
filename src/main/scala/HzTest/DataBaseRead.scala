package HzTest

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object DataBaseRead{
  def main(args:Array[String]) = {
    val url = "jdbc:mysql://localhost:3306/selftest"
    val tablename = "times"
    val user = "root"
    val password = "123456"
    val column = "time"
    val coltype = "date"
    val partition = "2"
    ReadSql(url,tablename,user,password,column,coltype,partition)
  }
  //读取mysql数据库
  def ReadSql(url:String,tablename:String,user:String,password:String,column:String,coltype:String,partition:String) = {
    val conf = new SparkConf().setAppName("bigdata test").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val prop = new Properties()
    prop.put("driver","com.mysql.jdbc.Driver")
    prop.put("url",url)
    prop.put("dbtable",tablename)
    prop.put("user",user)
    prop.put("password",password)
  //如果分区字段是long类型的，那么在读取mysql的时候，需要多加几个参数：列名，最小值，最大值，分区数
    if(coltype.toLowerCase() == "long"){
      val ab = LongTypeConn("com.mysql.jdbc.Driver",url,user,password,column,tablename)
      val lowerNum = ab(0)
      val upperNum = ab(1)
      val longFrame = spark.read.jdbc(
        prop.getProperty("url"),
        prop.getProperty("dbtable"),
        column,lowerNum,upperNum,
        partition.toInt,prop
      )
      longFrame.write.mode(SaveMode.Overwrite).json("C:\\Users\\86186\\Desktop\\out")
    }
    //如果是时间类型的，那么在读取的时候需要多一个参数，就是我们自定义划分的时间区间
    else if(coltype.toLowerCase() == "date"){
      val arr = DateTypeConn("com.mysql.jdbc.Driver",url,user,password,column,tablename,partition.toInt)
      val dateFrame = spark.read.jdbc(
        prop.getProperty("url"),
        prop.getProperty("dbtable"),
        arr,prop)
      dateFrame.write.mode(SaveMode.Overwrite).json("C:\\Users\\86186\\Desktop\\out")
    }
    spark.close()
  }

  /*
    如果分区字段是Long类型的数据，比如id，那么我们需要得到该字段的最大和最小值，再根据设置的分区个数进行分区
 */
  def LongTypeConn(driver: String, url: String, user:String, password: String, column: String, tablename: String):ArrayBuffer[Long] = {
    var conn:Connection = null
    val array = new ArrayBuffer[Long]()
    try{
      Class.forName(driver)
      conn = DriverManager.getConnection(url,user,password)
      val stat = conn.createStatement()
      val rs = stat.executeQuery("select min(" +column +") as minNum,max(" + column + ") as maxNum from " + tablename)
      while (rs.next()){
        val minNum = rs.getLong("minNum")
        val maxNum = rs.getLong("maxNum")
        array.append(minNum)
        array.append(maxNum)
      }
      return array
    }catch {
      case e:Exception => e.printStackTrace()
        return array
    }
    conn.close()
    array
  }

  /*
    如果分区字段是时间类型的，那么我们需要将数据表中的时间字段划分成一个个的时间段，并放到一个数组中
    */
  def DateTypeConn(driver: String, url: String, user: String, password: String, column: String, tablename: String, partition: Int):Array[String] ={
    var conn:Connection = null
    val array = new ArrayBuffer[String]()
    val resArray = ArrayBuffer[(String,String)]()
    var lastArray = Array[String]()
    try{
      Class.forName(driver)
      conn = DriverManager.getConnection(url,user,password)
      val stat = conn.createStatement()
      val rs = stat.executeQuery("select min(" +column +") as minNum,max(" + column + ") as maxNum from " + tablename)
      while (rs.next()){
        val minNum = rs.getString("minNum")
        val maxNum = rs.getString("maxNum")
        array.append(minNum)
        array.append(maxNum)
      }
      /*
      因为有很多种时间格式，所以在具体开发过程中，我们需要根据我们自己的数据格式进行处理，此处列举三种常见的时间格式
       */
      if(array(0).contains("-")){
        val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        var minTime = sf.parse(array(0)).getTime()
        val maxTime = sf.parse(array(1)).getTime()
        val subNum = (maxTime - minTime)/partition.toLong
        var midNum = minTime
        for(i <- 0 to partition - 1){
          minTime = midNum
          midNum = midNum + subNum
          if(i == 0){
            resArray.append(sf.format(minTime) -> sf.format(midNum))
          }else if(i == partition - 1){
            resArray.append(sf.format(minTime) -> sf.format(maxTime))
          }else{
            resArray.append(sf.format(minTime) -> sf.format(midNum))
          }
        }
      }else if(array(0).contains("/")){
        val sf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
        var minTime = sf.parse(array(0)).getTime()
        val maxTime = sf.parse(array(1)).getTime()
        val subNum = (maxTime - minTime)/partition.toLong
        var midNum = minTime
        for(i <- 0 to partition - 1){
          minTime = midNum
          midNum = midNum + subNum
          if(i == 0){
            resArray.append(sf.format(minTime) -> sf.format(midNum))
          }else if(i == partition - 1){
            resArray.append(sf.format(minTime) -> sf.format(maxTime))
          }else{
            resArray.append(sf.format(minTime) -> sf.format(midNum))
          }
        }
      }else{
        val sf = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
        var minTime = sf.parse(array(0)).getTime()
        val maxTime = sf.parse(array(1)).getTime()
        val subNum = (maxTime - minTime)/partition.toLong
        var midNum = minTime
        for(i <- 0 to partition - 1){
          minTime = midNum
          midNum = midNum + subNum
          if(i == 0){
            resArray.append(sf.format(minTime) -> sf.format(midNum))
          }else if(i == partition - 1){
            resArray.append(sf.format(minTime) -> sf.format(maxTime))
          }else{
            resArray.append(sf.format(minTime) -> sf.format(midNum))
          }
        }
      }

      //划分时间区间
      lastArray = resArray.toArray.map {
        case (start,end) =>
          column + s" >= '$start'" + " and " + column + s" <= '$end'"
      }
      return lastArray
    }catch {
      case e:Exception => e.printStackTrace()
        return lastArray
    }
    conn.close()
    lastArray
  }

}