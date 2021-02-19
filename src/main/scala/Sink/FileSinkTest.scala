package Sink

import Transform.SensorReading01
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._


/**
 * 向文件写入数据
 */
object FileSinkTest {

  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //如果想保证数据处理的顺序 那么就需要设置程序运行并行度为1，
    // 如果不设置则按照cpu核数来进行计算
    environment.setParallelism(1)
    //读取本地文件
    val inputData = "E:\\BIGDATAWORK\\Flink_scala\\data\\file.txt"
    val inputDataStream = environment.readTextFile(inputData)
    val dataStream = inputDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading01(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    //print 也是一个sink操作
    dataStream.print()
   /* 1弃用的方式 dataStream.writeAsCsv("E:\\BIGDATAWORK\\Flink_scala\\data\\out.txt")*/
    //2最新的使用方式
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("E:\\BIGDATAWORK\\Flink_scala\\data\\out1.txt"),
        new SimpleStringEncoder[SensorReading01]()
      ).build()
    )

    //文件输出操作
    environment.execute("file sink ...")

  }
}
