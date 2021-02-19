package Transform

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

//import _root_.SensorReading 引入另一个包中的类

//定义一个样例类 (模拟温度传感器)
case class SensorReading01(id: String, temp: Long, age: Double)

/**
 * 算子介绍
 */
object Transform01 {

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

     //.filter(new myFilter)

    //1简单的聚合操作
    var aggStream = dataStream
      .keyBy("id") //键控流
      .minBy("temp")
    /*aggStream.print()*/

    //2求出最小温度下的 最大的时间戳，要用reduce方法
    val resultStream = dataStream
      .keyBy("id") //
      /*
        2.1用Lambda方式求出 最新时间的最小温度
       .reduce((currentData,newData)=>{
         //当前的id,最新的时间，当前的温度与最新温度做比较 拿到最小值
         SensorReading01(currentData.id,newData.temp,currentData.age.min(newData.age))
       })*/
      //2.2自定义类实现
      .reduce(new myReduceFunction)

    /**
     * resultStream 使用reduce方法  用Lambda方式求出 最新时间的最小温度。(使用函数得出结果一夜)
     *
     * SensorReading01(hello_1,1547718199,32.2)
     * SensorReading01(hello_1,1547718201,32.1)
     * SensorReading01(hello_3,1547718202,32.4)
     * SensorReading01(hello_1,1547718203,32.1)
     * SensorReading01(hello_1,1547718204,32.1)
     * SensorReading01(hello_4,1547718198,32.3)
     */

    /*resultStream.print()*/

    //3多流转换操作
    //分流讲传感器温度分为高温区 与 低温区 俩条流

    val resultStream1 = dataStream.split(data => {
      if (data.age > 30.0) Seq("hight") else Seq("low")
    })

    val hightStream1 = resultStream1.select("hight")
    val lowStream1 = resultStream1.select("low")
    val allStream1 = resultStream1.select("low", "hight")

    //分流操作输出
    /* hightStream1.print("hight")
     lowStream1.print("low")
     allStream1.print("all")*/

    //4 合流操作 .connect
    //首先我们模拟一个 报警流 为了区分与其他流的数据格式不一样做调整
    val waringStream = hightStream1.map(data => (data.id, data.age))
    //.connect 与union 的区别  .connect 是可以合成相同或者不同的数据类型，而 union 则必须是相同的数据类型
    // connect 灵活复杂且接收一个流 ，union简单且接收多个数据流
    val connectedStream = waringStream.connect(lowStream1)

    val coMapResultStream = connectedStream
      .map(
        warinData => (warinData._1, warinData._2, "waring"),
        lowtempData => (lowtempData.id, "healthy")
      )


    //union 则必须是相同的数据类型
    val unionStream=hightStream1.union(lowStream1)

    /**
     * 合流后的输出
     */
    coMapResultStream.print("coMap")
    /**
     * coMap> (hello_1,32.2,waring)
     * coMap> (hello_1,healthy)
     * coMap> (hello_3,32.4,waring)
     * coMap> (hello_3,healthy)
     * coMap> (hello_1,32.7,waring)
     * coMap> (hello_4,healthy)
     * coMap> (hello_1,32.8,waring)
     * coMap> (hello_4,32.3,waring)
     */
    environment.execute("Execution  job...")


  }

}

/**
 * 实现一个自己的ReduceFunction
 */
class myReduceFunction extends ReduceFunction[SensorReading01] {
  override def reduce(value1: SensorReading01, value2: SensorReading01): SensorReading01 = {
    //使用我们自己实现的一个展示数据结构
    SensorReading01(value1.id, value1.temp.max(value2.temp), value1.age.min(value2.age))
  }
}

//自定义过滤
class  myFilter extends  FilterFunction[SensorReading01]{
  override def filter(value: SensorReading01): Boolean =
    value.id.startsWith("hello_1")
}

//定义Rich 类
class  myRichMapper extends RichMapFunction[SensorReading01,String]{

  override def open(parameters: Configuration): Unit =  {
   // getRuntimeContext 生命周期  open 一般用来初始化连接池 数据等操作
   // print(getRuntimeContext)
  }

  //每条数据做一次 MAP操作
  override def map(value: SensorReading01): String =
    value.id+"temp"
}

