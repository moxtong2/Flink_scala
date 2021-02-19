package Sink

import Transform.SensorReading01
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * 向kafka 写入数据
 */
object KafkaSinkTest {

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
   /* dataStream.print()*/
    /*val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.20.6.98:9092")
    properties.setProperty("group.id", "iteblog")
    properties.setProperty("auto.offset.reset", "latest")*/



    environment.execute("kafka sink test ...")

  }
}
