package CustomSink

import Transform.SensorReading01
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RichFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverAction, DriverManager, PreparedStatement}

/**
 * 自定义MysqlSink
 */
object CustomMySqlJDBCSink {

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


    dataStream.addSink(new myJDBCSinkFunction())

    environment.execute("CustomMySqlJDBCSink  sink test ...")

  }

}

class myJDBCSinkFunction extends RichSinkFunction[SensorReading01]{
   //建立连接
   var  conn: Connection = _
   var  insertStmt: PreparedStatement= _
   var  updateStmt: PreparedStatement= _

  //定义连接 预编译语句
  override def open(parameters: Configuration): Unit = {
    conn= DriverManager.getConnection("jdbc:mysql://10.20.6.138:3306/sensor01","root","Qaz123!@#")
    insertStmt= conn.prepareStatement("INSERT  INTO sensor_temp (id,temp)values(?,?)")
    updateStmt= conn.prepareStatement("UPDATE sensor_temp set temp=? where  id= ?")
  }

  //invoke 每来一条数据要做的事情
  override def invoke(value: SensorReading01, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setString(1,value.age.toString)
    updateStmt.setString(2,value.id)
    updateStmt.execute()


    print("updateStmt.getUpdateCount==>"+ updateStmt.getUpdateCount)

    if (updateStmt.getUpdateCount==0){
      insertStmt.setString(1,value.id)
      insertStmt.setString(2,value.age.toString)
      insertStmt.execute()
   }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
