package Sink

import Transform.SensorReading01
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, FlinkKafkaProducer011}
import org.apache.flink.streaming.util.serialization.{KeyedSerializationSchema, SerializationSchema}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.collection.parallel.defaultTaskSupport.environment

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
        SensorReading01(arr(0), arr(1).toLong, arr(2).toDouble).toString
      })

    /*1  使用KafkaProducer 方式推送数据  val properties = new Properties()
     properties.setProperty("bootstrap.servers", "10.20.6.98:9092")
     properties.setProperty("group.id", "iteblog")
     properties.setProperty("auto.offset.reset", "latest")
     properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
     properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
     val producer = new KafkaProducer[String, String](properties)
     producer.send(new ProducerRecord("test",dataStream.toString()))*/

    //scala flink 中的sink 是flink三大逻辑结构之一（source，transform，sink）,功能就是负责把flink处理后的数据输出到外部系统中，
    // flink 的sink和source的代码结构类似
    //自定义序列化 向kafka推送数据
    /*2 dataStream.addSink(new FlinkKafkaProducer[SensorReading01]("10.20.6.98:9092","test", new MySchema))*/

    //3第三种方式没找到    这个类不行FlinkKafkaProducer001
    /*dataStream.addSink(new FlinkKafkaProducer011[String]("10.20.6.98:9092","test", new SimpleStringSchema()))*/
    environment.execute("kafka sink test ...")

  }

  /**
   * 自定义序列化（这里可有处理）
   */
  class MySchema extends KeyedSerializationSchema[SensorReading01] {

    //设置key
    override def serializeKey(t: SensorReading01): Array[Byte] = t.id.getBytes()

    //此方法才是实际底层produce的topic，FlinkKafkaProducer011中的topic_name级别不如此级别
    //这个Topic 可以自定义 实现 动态传输到不同的topic，以进行数据的分类
    override def getTargetTopic(t: SensorReading01): String = "test"

    //保留原始数据 原封不动推送
    override def serializeValue(t: SensorReading01): Array[Byte] = t.toString.getBytes

    //自定义推送格式
    /*override def serializeValue(t: SensorReading01): Array[Byte] = {
      {"id :"+t.id+"temp:"+t.temp+""}.getBytes()
    }*/
  }




}
