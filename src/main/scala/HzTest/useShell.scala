package HzTest

import java.io.{BufferedReader, InputStreamReader}

object useShell {
  def main(args: Array[String]): Unit = {
    val path = "D:\\software\\idea\\WorkSpace\\selfTest\\src\\main\\Shell\\test.sh"
    val res = executeShell(path)
    println(res)
  }

  def executeShell(shpath: String): Unit = {
    try {
      val ps = Runtime.getRuntime.exec(shpath)
      ps.waitFor()
      val br = new BufferedReader(new InputStreamReader(ps.getInputStream()))
      val sb = new StringBuffer()
      var line = ""
      while ((line = br.readLine) != null){
        sb.append(line).append("\n")
      }
      val result = sb.toString
      System.out.println(result)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
