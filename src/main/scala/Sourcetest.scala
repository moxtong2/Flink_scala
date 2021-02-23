import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
/*import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
*  这个是第一种实现方式
*
*  如下包的实现方式
*   <!--   <dependency>
*               <groupId>org.apache.flink</groupId>
*               <artifactId>flink-connector-kafka_2.12</artifactId>
*               <version>1.10.1</version>
*           </dependency>-->
*   FlinkKafkaConsumer010是另一种实现方式
* */

import java.util.Properties
import scala.util.Random

//定义一个样例类 (模拟温度传感器)
case class SensorReading(id: String, time: Long, name: String, age: Double, hei: Float, ispass: Boolean)

/**
 * 从集合中读取数据
 */
object Sourcetest {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //添加数据集
    val dataList = List(
      SensorReading("1", System.currentTimeMillis(), "tom", 19.1, 23.43f, true),
      SensorReading("2", System.currentTimeMillis(), "tom1", 19.1, 23.43f, true),
      SensorReading("3", System.currentTimeMillis(), "tom2", 19.1, 23.43f, true),
      SensorReading("4", System.currentTimeMillis(), "tom3", 19.1, 23.43f, true)
    )
    //TypeInformation 隐式转换
    val value: DataStream[SensorReading] = environment.fromCollection(dataList)
    //输出单个
    println(environment.fromElements(1, "tom"))
    //从集合中读取数据 不保证数据顺序
    value.print()

    //指定读取文件
    val inputPath: String = "E:\\BIGDATAWORK\\Flink_scala\\data\\file.txt"
    //读取到的文件
    val value1 = environment.readTextFile(inputPath)
    value1.print()


    //从kafka中读取数据 需要导入 flink 从kafka读入数据的依赖 （连接器)
    //(new FlinkKafkaConsumer[String]("neo4j",)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.20.6.98:9092")
    properties.setProperty("group.id", "iteblog")
    properties.setProperty("auto.offset.reset", "latest")
    val value2 = environment.addSource(new FlinkKafkaConsumer010[String]("test", new SimpleStringSchema(), properties))
    value2.print()

    //自定义数据源
    val value3 = environment.addSource(new MySensorSource())
    value3.print()

    //执行
    environment.execute("data List ....")
  }

  //继承SourceFunction 函数 类型是SensorReading类型
  class MySensorSource extends SourceFunction[SensorReading] {
    //定义一个变量标志位 用来标识数据源是否正常发送数据
    var running: Boolean = true

    override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
      //定一个随机数发生器
      val rand = new Random()
      //定义生成一组10个 传感器初始温度值 （id,temp）
      var curTemp = 1.to(10).map(i => ("sensor_" + i, rand.nextDouble() * 100))


      //定义一个无限的循环  不停产生数据
      while (running) {
        //在此前基础上 更新温度值 nextGaussian使用正态分布随机数
        curTemp = curTemp.map(
          data => (data._1, data._2 + rand.nextGaussian())
        )
        var curTime = System.currentTimeMillis()
        //SensorReading("1", System.currentTimeMillis(), "tom", 19.1, 23.43f, true), 使用sourceContext.collect 收集 发送数据
        curTemp.foreach(
          data => sourceContext.collect( SensorReading(data._1,curTime,"tom",19.1,23.43f,true))
        )
        //间隔一段时间
        Thread.sleep(1000)
      }

    }

    //当取消时对变量状态进行修改
    override def cancel(): Unit = running = false

    //23
  }

}
