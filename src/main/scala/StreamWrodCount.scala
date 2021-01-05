import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, createTypeInformation}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 开启一个流式处理
 * 使用 nc 模拟开始 7777端口
 * windows 下nc的使用方式
 * https://blog.csdn.net/nicolewjt/article/details/88898735
 */
object StreamWrodCount {

  def main(args: Array[String]): Unit = {
       //创建流处理执行环境
       val environment :StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
       //得到一个socket文本流
       val inputDataStream :DataStream[String] = environment.socketTextStream("127.0.0.1", 7777)
       //转化并统计
       val resultDataStream : DataStream[(String,Int)]  = inputDataStream
         .flatMap(_.split(" ")) //flatMap与filter 的区别是什么呢
         .filter(_.nonEmpty)
         .map((_,1))
         .keyBy(0)     //keyBy 与groupBy的区别是什么呢  keyBy 会进行数据数据重分区？ hashCoed 取余？
         .sum(1)   // 对当前分组的所有数据的第二个元素求和

         resultDataStream.print()
       //启动执行任务
      environment.execute("Stream job")

      //设置线程并行度
     // environment.setParallelism(4)
    // 顺序写入 resultDataStream.print().setParallelism(1)

    //当我们ip 与端口 不能写死的情况下 我们需要获取命令参数 flink 给我们提供了工具类
   /* val parameterTool:ParameterTool = ParameterTool.fromArgs(args)
    val hostStr:String = parameterTool.get("host")
    val portStr:Int = parameterTool.getInt("port")*/
   // 需要在 环境中做一个配置 --host 127.0.0.1 --port 7777  启动命令参数中配置


    //flink 分布式架构带来的乱序问题

  }
}
