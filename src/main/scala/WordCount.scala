import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}

object WordCount {

  def main(args: Array[String]): Unit = {
     //生成读取环境
    val env= ExecutionEnvironment.getExecutionEnvironment
    //指定读取文件
    val inputPath :String  = "E:\\BIGDATAWORK\\Flink_scala\\data\\test.txt"
    //读取到的文件
    val value :DataSet[String] = env.readTextFile(inputPath)
    //拿到文件进行分析
    val  resultDataSet:DataSet[(String,Int)] =value
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0) //以第一个元素进行分组统计
      .sum(1)   // 对当前分组的所有数据的第二个元素求和

    resultDataSet.print()
  }
}
