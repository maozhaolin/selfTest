import org.apache.spark.deploy.SparkSubmit;

public class UseJar {
    public static void main(String[] args) {
        String[] arg0 = new String[]{
                "--master", "spark://Hadoop001:7077"
                ,"--deploy-mode", "client"
                ,"--name", "test java submit job to spark"
                ,"--class", "HzTest.ChangeColumn"//指定spark任务执行函数所在类
                ,"--executor-memory", "1G"//运行内存
                ,"D:\\software\\idea\\WorkSpace\\com.program.UserBehaviorAnalysis\\target\\com.program.UserBehaviorAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar"
                ,
        };

        SparkSubmit.main(arg0);
    }
}
