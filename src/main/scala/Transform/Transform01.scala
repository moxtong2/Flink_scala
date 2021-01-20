package Transform

import org.apache.flink.streaming.api.scala._

//定义一个样例类 (模拟温度传感器)
case class SensorReading(id: String,  temp: Long, age: Double)

/**
 *  算子介绍
 */
object Transform01 {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //读取本地文件
    val  inputData ="E:\\BIGDATAWORK\\Flink_scala\\data\\file.txt"
    val inputDataStream = environment.readTextFile(inputData)
    val dataStream=inputDataStream
        .map( data =>{
        val arr =data.split(",")
        SensorReading(arr(0),arr(1).toLong,arr(2).toDouble)
        })
    var aggStream =dataStream
      .keyBy("id")
      .min("temp")

    aggStream.print()
    environment.execute("Execution  job...")


  }

}
