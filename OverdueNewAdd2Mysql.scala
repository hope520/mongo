package cn.fulin

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONArray, JSONException}
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

/**
  * Created by Administrator on 2018/5/2.
  */
object OverdueNewAdd2Mysql {
  private val TOPIC_NAME = "overdue_increment" //topic名
  private val GROUPID_NAME = "overdue_increment" //groupid名


  private val logger = LoggerFactory.getLogger(OverdueNewAdd2Mysql.getClass)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OverdueNewAdd2Mysql")
      .setMaster("local[*]")
      //每个分区最大的消费数据条数,避免冷启动的情况下拉取数据过大,过猛
      .set("spark.streaming.kafka.maxRatePerPartition", "2")
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
      "bootstrap.servers" -> "master.fulin.com:9092,slave1.fulin.com:9092,slave2.fulin.com:9092",
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest",
      "enable.auto.commit" -> "false"
    )


    //通过自定义的方法查询数据库获取偏移量
    val fromOffsets = OffsetGetFromMysql(groupId)

    //获取当日时间
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var date = dateFormat.format(now)

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


        //获取kafka中的数据的用户电话号码
        val phoneNoRdd: RDD[NewBean] = filteredRDD.map(bean => {
          val num: String = bean.mobile

          println(num)
          //通过用户电话号码,从mongo中获取联系人联系频次最大的top10
          val callRecordTopTenAndAdd: String = getPhoneNumAndAddFromMongodb(num)
          println(callRecordTopTenAndAdd)
          val pNumAndAdd: Array[String] = callRecordTopTenAndAdd.split("-")

         val add= pNumAndAdd(1)
          val pNum= pNumAndAdd(0)

          getFullUserInfo(bean, add, callRecordTopTenAndAdd,date)
        })

        //phoneNoRdd.foreach(println)

        //得到用户户籍地址
        //        val arr2: JSONArray = obj.getJSONArray("cell_phone")
        //        val address = GetJsonData.getAddress(arr2)
        //将联系人联系频次最大的top10与kafka中的联系人数据合并存入mysql
        phoneNoRdd.foreach(WriteSql.writeDataInMysql(_))


        //得到电话号码
        //val num: String = phoneNoRdd.first()


        //通过自定义的方法将偏移量放入数据库
        OffsetGetFromMysql(offsetRanges, groupId)

      }
    })
    //启动SparkStreaming
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 得到用户完整信息
    * @param bean
    * @param add
    * @param pNum
    * @return
    */
  private def getFullUserInfo(bean: NewBean, add: String, pNum: String,date: String) = {
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
      bean.companyAddress,
      bean.permanentAddress,
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
    * 通过用户电话号码,从mongo中获取联系人联系频次最大的top10和户籍地址
    *
    * @param num
    * @return
    */
  private def getPhoneNumAndAddFromMongodb(num: String) = {
    val spark = SparkSession.builder()
      //.master("local[*]")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://101.132.38.249:27017/thirdpart.MOXIE_CARRIER?readPreference=secondaryPreferred")
      .getOrCreate()

    val docsRDD: MongoRDD[Document] = MongoSpark.load(spark.sparkContext)
    //推荐使用（先向MongoDB发送查询条件，然后将过滤后的数据返回，减少了数据的传输，提升了Spark运行的效率）
    val filtered: MongoRDD[Document] = docsRDD.withPipeline(Seq(Document.parse("{$match:{\"baseInfo.mobile\":{$eq:\"" + num + "\"}}}")))
    val df: DataFrame = filtered.toDF()

    val take = df.takeAsList(1).toString

    val callRecordTopTenAndAdd = if ("[]" == take) {
      "未知-未知"
    } else {
      df.createOrReplaceTempView("MOXIE")
      val result: DataFrame = spark.sql("SELECT explode (mxReport.call_contact_detail) as tel FROM MOXIE")

      val cpDF: DataFrame = spark.sql("SELECT explode (mxReport.cell_phone) as tel FROM MOXIE ")
      cpDF.createOrReplaceTempView("cpDF")

      result.createOrReplaceTempView("result")
      val result2: DataFrame = spark.sql(
        """
          |select tel.peer_num
          |from result limit 10
        """.stripMargin)

      val res = result2.takeAsList(10).toString

      val add: DataFrame = spark.sql(
        """
          |select tel.value
          |from cpDF
          |where tel.key like 'address'
        """.stripMargin)

      val address = add.takeAsList(1).toString


      res + "-" + address

    }
    callRecordTopTenAndAdd
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
}
