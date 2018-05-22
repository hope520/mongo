package cn.fulin

import java.text.SimpleDateFormat
import java.util.Date

import cn.fulin.OverdueNewAdd2Mysql.getFullUserInfo
import com.alibaba.fastjson.{JSON, JSONArray, JSONException, JSONObject}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import org.slf4j.LoggerFactory
import test.GetJsonData

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/5/2.
  */
object OverdueNewAdd2Mysql2 {
  private val TOPIC_NAME = "overdue_increment" //topic名
  private val GROUPID_NAME = "overdue_increment" //groupid名


  private val logger = LoggerFactory.getLogger(OverdueNewAdd2Mysql2.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OverdueNewAdd2Mysql2")
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

        val newBeanRDD: RDD[NewBean] = parseJson(lines)

        //过滤
        val filteredRDD: RDD[NewBean] = newBeanRDD.filter(bean => bean != null)


        //通过用户电话号码,得到全量用户信息
        val userInfoRdd: RDD[NewBean] = filteredRDD.map(bean => {
          val num: String = bean.mobile
          val userInfo: String = GetUserInfoFromHBase.getHbasedata(num, "MOXIE_CARRIER")
          //println(userInfo)

          //得到用户联系频次最大的top10和户籍地址
          val json: JSONObject = JSON.parseObject(userInfo)
          println(json)



          if (null == json) {
            val callRecordTopTen = "未知"
            val address = "未知"
            val cAddAndPAdd = "未知"
            getFullUserInfo(bean, address, callRecordTopTen,cAddAndPAdd)
          } else {
            val obj = json.getJSONObject("mxReport")
            //println(obj)

            if (null == obj) {
              val callRecordTopTen = "未知"
              val address = "未知"
              val cAddAndPAdd = "未知"
              getFullUserInfo(bean, address, callRecordTopTen,cAddAndPAdd)
            } else {
              val arr: JSONArray = obj.getJSONArray("call_contact_detail")
              val callRecordTopTen = GetJsonData.getTopTen(arr)
              //val callRecordTopTen: String = getRecord(arr)

              //得到用户户籍地址
              val arr2: JSONArray = obj.getJSONArray("cell_phone")
              val address = GetJsonData.getAddress(arr2)

              val arr3: JSONArray = obj.getJSONArray("user_basic")
              val cAddAndPAdd = GetJsonData.getCAddAndPAdd(arr3)

              getFullUserInfo(bean, address, callRecordTopTen,cAddAndPAdd)
            }


          }

        })

        //通过用户电话号码,从mongo中获取联系人联系频次最大的top10
        //        val callRecordTopTen: String = getPhoneNumFromMongodb(num)
        //将联系人联系频次最大的top10与kafka中的联系人数据合并存入mysql
        //userInfoRdd.foreach(WriteSql.writeDataInMysql(_, date))

        userInfoRdd.foreach(bean=>{
            WriteSql.writeDataInMysql(bean)
            //WriteSql.writeHisData2Mysql(bean)

        })

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
  private def parseJson(lines: RDD[String]) = {
    //将jons字符串转换成JSON对象
    val newBeanRDD = lines.map(line => {

      var newBean: NewBean = null
      try {
        newBean = JSON.parseObject(line, classOf[NewBean])
      } catch {
        case e: JSONException => {
          //logger记录log
          logger.error(s"parse json err: $line")
        }
      }
      newBean
    })
    newBeanRDD
  }


  /**
    * 得到用户完整信息
    *
    * @param bean
    * @param add
    * @param pNum
    * @return
    */
  private def getFullUserInfo(bean: NewBean, add: String, pNum: String,cAddAndPAdd:String) = {
    //获取当日时间
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    //val date = dateFormat.format(now)
    val date: String = dateFormat.format(now)


    NewBean(
      bean.age,
      bean.balance,
      bean.bankCard,
      bean.cardId,
      bean.contacts,
      bean.contactsPhone,
      bean.contactsRemark,
      bean.contactsType,
      bean.entrustAmt,
      bean.entrustEndDate,
      bean.entrustStartDate,
      bean.lastRepayTime,
      bean.loanAmt,
      bean.loanDate,
      bean.mobile,
      bean.name,
      bean.orgName,
      bean.overdueAmt,
      bean.overdueDate,
      bean.overdueDays,
      bean.overdueId,
      bean.overdueStatus,
      bean.overdueTotalAmt,
      bean.productName,
      bean.realAmt,
      bean.sex,
      bean.stages,
      bean.status,

      add,
      pNum,
      bean.seqNo,
      bean.address2,
      cAddAndPAdd,
      cAddAndPAdd,
      bean.companyName,
      bean.repayAmt,
      bean.lastRepayAmt,
      bean.repayNum,
      bean.homePhone,
      bean.companyPhone,
      bean.qq,
      bean.weChat,
      bean.email,
      bean.aliId,
      bean.contacts2,
      bean.contactsType2,
      bean.contactsPhone2,
      bean.contactsRemark2,
      bean.allotId,
      bean.loanType        ,
      bean.collectionType  ,
      bean.billDate        ,
      bean.interestRate    ,
      bean.startSeatNo,
      date,
      date,
      bean.contracNo

    )
  }



  /**
    * 得到通讯录top10
    *
    * @return
    */
  private def getRecord(arr: JSONArray) = {

    var list = ListBuffer[(Int,Int)]()
    for (i <- 0 to arr.size() - 1) {
      val job = arr.getJSONObject(i)
      val num = job.getString("call_cnt_6m").toInt
      val pNum = job.getString("peer_num").toInt
      list.append((num,pNum))
    }

    val tp: String = list.sortBy(_._1).take(10).map(_._2).toString().replace("ListBuffer","")


    tp

  }






}
