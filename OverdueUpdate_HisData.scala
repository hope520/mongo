package cn.fulin

import java.text.SimpleDateFormat
import java.util.Date

import cn.fulin.WriteSql.LOG
import com.alibaba.fastjson.{JSON, JSONArray, JSONException, JSONObject}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scalikejdbc.{DB, SQL}
import java.text.SimpleDateFormat
import java.util.Calendar
import test.GetJsonData

/**
  * Created by Administrator on 2018/5/2.
  */
object OverdueUpdate_HisData {
  private val TOPIC_NAME = "overdue_update" //topic名
  private val GROUPID_NAME = "overdue_update" //groupid名


  private val logger = LoggerFactory.getLogger(OverdueUpdate_HisData.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
       // .setAppName(OverdueUpdate_HisData.getClass.getSimpleName)
      .setAppName("OverdueUpdate_HisData")
      //.setMaster("local[*]")
      //每个分区最大的消费数据条数,避免冷启动的情况下拉取数据过大,过猛
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.stopGracefullyOnShutdown", "true") //以优雅的方式结束driver,避免数据丢失
    val sc: SparkContext = new SparkContext(conf)


    //StreamingContext创建
    val ssc = new StreamingContext(sc, Seconds(2))
    var groupId = GROUPID_NAME //生产集群使用的kafka的groupid
    //读取kakfa的主题
    val topic = Set(TOPIC_NAME) //生产使用的topic
    //设置kafka的参数
    var KafkaParams = Map[String, String](
      //118.31.70.236,47.98.186.236,47.97.183.235
      "bootstrap.servers" -> "slave1.fulin.online.com:9092,slave2.fulin.online.com:9092,slave3.fulin.online.com:9092",
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest",
      "enable.auto.commit" -> "false"
    )


    //通过自定义的方法查询数据库获取偏移量
    val fromOffsets = OffsetGetFromMysql(groupId)



    //第一次启动
    var stream = if (fromOffsets.size == 0) {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, KafkaParams, topic)
    } else {
      /**
        * 定义一个map用来存储比较后的偏移量
        */
      var checkOffset = Map[TopicAndPartition, Long]()
      //第二次启动所以可以校验偏移量的有效性，谁大用谁
      val kc = new KafkaCluster(KafkaParams)
      //指定某个主题的某个分区下的最早有效偏移量
      val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(fromOffsets.keySet)
      if (earliestLeaderOffsets.isRight) {
        //获取某个主题某个分区下面的LeaderOffset
        val partitionToOffset: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = earliestLeaderOffsets.right.get
        //开始比较自己持有的某个主题某个分区的偏移量和kafka持有的最早偏移量做比较
        fromOffsets.foreach(dbOffset => {
          //kafka记录的最早偏移量
          val kcOffset = partitionToOffset.get(dbOffset._1).get.offset
          //如果kafka中的最早偏移量比我们自身的偏移量大
          if (kcOffset > dbOffset._2) {
            checkOffset += ((dbOffset._1, kcOffset))

          } else {
            checkOffset += dbOffset
          }
        })
      }
      /**
        * 以上均为校验自身持有的偏移量和kafka中持有的最早偏移量做比较
        */
      //第二次启动
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message()) //每条kafka的数据进来经过这个函数的处理
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc,
        KafkaParams, checkOffset, messageHandler)
    }

    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        //获取每个分区的偏移量
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //开始业务逻辑
        val lines: RDD[String] = rdd.map(_._2)

        val updateBeanRDD: RDD[UpdateBean] = parseUpdateBeanJson(lines)

        //过滤
        val filteredRDD: RDD[UpdateBean] = updateBeanRDD.filter(bean => bean != null)


        filteredRDD.foreach(updateBean=>{
          if (updateBean.status.equals("CLOSE")){
            writeData2MysqlUpdateRepay(updateBean)
            println(updateBean)
          }
          else{
            WriteSql.writeData2MysqlUpdate(updateBean)
            //println(updateBean)
          }
        })
       // filteredRDD.foreach(WriteSql.writeData2MysqlUpdate(_))

        //通过自定义的方法将偏移量放入数据库
        OffsetGetFromMysql(offsetRanges, groupId)

      }
    })
    //启动SparkStreaming
    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 解析Json
    *
    * @param lines
    * @return
    */
  private def parseUpdateBeanJson(lines: RDD[String]) = {
    //将jons字符串转换成JSON对象
    val updateBeanRDD = lines.map(line => {

      var updateBean: UpdateBean = null
      try {
        updateBean = JSON.parseObject(line, classOf[UpdateBean])
      } catch {
        case e: JSONException => {
          //logger记录log
          logger.error(s"parse json err: $line")
        }
      }
      updateBean
    })
    updateBeanRDD
  }


  //更新Update表中相应字段
  def writeData2MysqlUpdateRepay(bean: UpdateBean) = {

    //获取当日时间
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //val date = dateFormat.format(now)
    val date: String = dateFormat.format(now)

      LOG.info(s"-----$bean-----")
      if (bean.mobile != null) {
        DB.localTx({
          implicit session =>
            SQL(
              """
                |UPDATE detail_overdue_days_report SET
                |last_repay_amt = ?,
                |last_repay_time = ?,
                |phone = ?,
                |overdue_days = ?,
                |seq_no = ?,
                |remark = ?,
                |
                |balance = ?,
                |overdue_amt = ?,
                |overdue_total_amt = ?,
                |overdue_status = ?,
                |loan_type = ?,
                |collection_type =?,
                |repay_num = ?,
                |entrust_end_date = ?,
                |updated_date = ?
                |
                |WHERE overdue_id = ?
              """.stripMargin).bind(
              bean.lastRepayAmt,
              bean.lastRepayTime,
              bean.mobile,
              bean.overdueDays,
              bean.seqNo,//批次号
              bean.status,

              "0",
              "0",
              "0",
              "0",
              "信贷",
              "回收",
              "1",
              date,
              date,

              bean.overdueId

            ).update().apply()
        })
    }




  }


}
