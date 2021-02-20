package Sink

import Transform.SensorReading01
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * Redis 写入数据
 */
object RedisSinkTest {

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

    //FlinkJedisPoolConfig 这个是FlinkJedisConfigBase的子类 提供
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("10.20.7.122")
      .setPort(6379)
      .build()

    //dataStream 的类型要与 sin传入的类型保持一致
    dataStream.addSink(new RedisSink[SensorReading01](config,new myRedisSinkMapper))

    environment.execute("redis sink test ...")
  }

}

class  myRedisSinkMapper extends  RedisMapper[SensorReading01]{

  //定义保存数据 写入redis的命令 HSET 表名
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  }

  //获取key的数据
  override def getKeyFromData(data: SensorReading01): String = data.id
  //获取value数据
  override def getValueFromData(data: SensorReading01): String = data.age.toString
}

