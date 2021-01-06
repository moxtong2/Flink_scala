import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala._

case class SensorReading(id: Int, name: String, age: Double, hei: Float, ispass: Boolean)

object Sourcetest {

  def main(args: Array[String]): Unit = {

    //创建执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //添加数据集
    val dataList = List (
      SensorReading(1, "tom", 19.1, 23.43f, true),
      SensorReading(2, "tom1", 19.1, 23.43f, true),
      SensorReading(3, "tom2", 19.1, 23.43f, true),
      SensorReading(4, "tom3", 19.1, 23.43f, true)
    )
    val value = environment.fromCollection(dataList)

    value.print()

  }
}
