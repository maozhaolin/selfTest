package FlinkDemo
import org.apache.flink.api.scala._

object FlinkTest {

  //批处理代码
  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inpath = "D:\\software\\idea\\WorkSpace\\selfTest\\src\\main\\resources\\Hello.txt"
    val data: DataSet[String] = env.readTextFile(inpath)

    //分词之后做count处理
    val result: AggregateDataSet[(String, Int)] = data
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    result.print()


  }
}
