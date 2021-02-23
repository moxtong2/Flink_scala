package Sink

import Transform.SensorReading01
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.{Requests, RestClientBuilder}

import java.util

/**
 * Elastic Search 写入数据
 * 1通过有账号密码写入
 * 2无账号密码写入
 * 具体参数 可以使用配置文件  通过 ParameterTool 工具来获取 前端传入参数
 * 这里暂时写死，生产不推荐这种方式，如果可以 连接池这种思路 可以在open中使用起来
 */
object EsSinkTest {

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

    //定义es地址
    val httpHost = new util.ArrayList[HttpHost]()
    httpHost.add(new HttpHost("10.20.6.139", 9200))
    /* 多个的情况 如下添加 httpHost.add(new HttpHost("10.20.6.138", 9200))*/

    /*
    *
    *
    *
    * Header[] defaultHeaders = new Header[]{new BasicHeader("username", "password")};
    *     esSinkBuilder.setRestClientFactory(
    *    restClientBuilder -> {
    *        restClientBuilder.setDefaultHeaders(defaultHeaders);
    *     }
    *   );
    *   这种写法的用户名密码验证不行  ，会报如下错误 ：
    *   method [HEAD], host [https://XXXXXXXXXXXXXX:9243], URI [/],
    *   status line [HTTP/1.1 401 Unauthorized]
    *   at org.elasticsearch.client.RestHighLevelClient.parseResponseException(RestHighLevelClient.java:625)
    *
    * */

    // new MyElasticsearchSinkFunction 也可以使用匿名类的写法 如下：
    val myElasticsearchSinkFunction = new ElasticsearchSink.Builder[SensorReading01](httpHost,
      new ElasticsearchSinkFunction[SensorReading01]() {
        override def process(t: SensorReading01, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          //1包装一个map 作为dataSource
          val dataSource = new util.HashMap[String, String]()
          dataSource.put("id", t.id)
          dataSource.put("temp", t.temp.toString)

          //2创建index request 用于发送http请求
          val indexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata") //版本6以下需要设置？
            .source(dataSource)

          //3用requestIndexer 发送请求
          requestIndexer.add(indexRequest)
        }
      })

    //缓冲的最大操作数
    myElasticsearchSinkFunction.setBulkFlushMaxActions(1)

    /*
     * 设置关于es的认证方式  官方示例：https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/elasticsearch.html
     * 相关文章：https://www.it1352.com/1932540.html
     * provide a RestClientFactory for custom configuration on the internally created REST client
     * */
    myElasticsearchSinkFunction.setRestClientFactory(
      new RestClientFactory {
        override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
          restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
              // elasticsearch username and password
              val credentialsProvider = new BasicCredentialsProvider()
              credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "123456"))
              httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            }
          })
        }
      }
    )

    //构造一个完整的sink给这个流
    dataStream.addSink(myElasticsearchSinkFunction.build())

    environment.execute("elastic search  sink test ...")
  }
}

/*

//实现自定义连接es程序
class MyElasticsearchSinkFunction extends ElasticsearchSinkFunction[SensorReading01] {
  //每进来一次数据 处理过程
  override def process(t: SensorReading01, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

    //包装一个map 作为dataSource
    val dataSource = new util.HashMap[String, String]()
    dataSource.put("id", t.id)
    dataSource.put("temp", t.temp.toString)

    //创建index request 用于发送http请求
    val indexRequest = Requests.indexRequest()
      .index("sensor")
      .`type`("readingdata") //版本6以下需要设置
      .source(dataSource)

    //用requestIndexer 发送请求
    requestIndexer.add(indexRequest)

  }
}
*/

