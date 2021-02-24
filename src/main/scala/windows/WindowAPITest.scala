package windows

import Transform.SensorReading01
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowAPITest {

  def main(args: Array[String]): Unit = {


    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //如果想保证数据处理的顺序 那么就需要设置程序运行并行度为1，
    // 如果不设置则按照cpu核数来进行计算
    environment.setParallelism(1)
    //读取本地文件
    /*  val inputData = "E:\\BIGDATAWORK\\Flink_scala\\data\\file.txt"
      val inputDataStream = environment.readTextFile(inputData)*/

    val inputDataStream: DataStream[String] = environment.socketTextStream("127.0.0.1", 7777)

    val dataStream = inputDataStream
      .map(data => {
        val arr = data.split(",")
        SensorReading01(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    //每15统计各窗口内 窗
    val resultDataStream = dataStream.map(data => (data.id, data.age, data.temp))
      .keyBy(_._1)

      /** 按照二元组的第一个元素分组 .keyBy(data=>data._1) 这是一种写法 */
      //  .window(TumblingEventTimeWindows.of(Time.minutes(15))) //滚动时间窗口
      //  .window(SlidingEventTimeWindows.of(Time.minutes(15),Time.minutes(10)))//滑动窗口
      //  .window(EventTimeSessionWindows.withGap(Time.minutes(10)))//会话窗口 两个数据之间的间隔
      //  .countWindow(2)//滚动的窗口  两个参数时就是 滑动的窗口
      /*定义什么时候关闭 .trigger()*/
      /* 允许处理延迟的数据.allowedLateness()*/
      /*将迟到的数据放入侧输出流.sideOutputLateData()*/

      .timeWindow(Time.seconds(15)) //简写滚动时间窗口 秒
      // .minBy("age")
      //这个reduce 是操作map
      .reduce((currentData, newData) => (currentData._1, currentData._2.min(newData._2), newData._3))

    resultDataStream.print()

    environment.execute("WindowAPITest test ...")

  }
}

/**
 * 实现一个自己的ReduceFunction
 */
/*
class myReduceFunction extends ReduceFunction[SensorReading01] {
  override def reduce(value1: SensorReading01, value2: SensorReading01): SensorReading01 = {
    //使用我们自己实现的一个展示数据结构
    SensorReading01(value1.id, value1.temp.max(value2.temp), value1.age.min(value2.age))
  }
}*/
