package FlinkDemo

import java.security.Policy.Parameters

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamFlink {
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")

    //创建流处理的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //接收socket数据流
    val textDataStream = env.socketTextStream(host,port)

    //逐一读取数据，打散之后count
    val dataStream = textDataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)

    dataStream.print()

    //执行任务
    env.execute("StreamWordCount")

  }
}
