package cn.fulin

import kafka.common.TopicAndPartition
import org.apache.commons.logging.LogFactory
import org.apache.spark.streaming.kafka.OffsetRange
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
  * Created by zhuang on 2018/2/13.
  */
object OffsetGetFromMysql {
  var LOG = LogFactory.getLog(OffsetGetFromMysql.getClass)
  //加载配置信息
  DBs.setup()

  /**
    * 从数据库中获取上一个批次维护好的偏移量数据信息
    */
  def apply(groupId: String): Map[TopicAndPartition, Long] = {
    DB.readOnly {
      implicit session =>
        SQL("select * from bigdata_kafkaoffset where groupid = ?").bind(groupId)
          .map(rs => (new TopicAndPartition(rs.string("topic"), rs.string("kafka_partition").toInt), rs.string("kafka_offset").toLong))
          .list().apply()
    }.toMap
  }

  /**
    * 将每个分区的偏移量放入数据库
    */
  def apply(offsetRanges:Array[OffsetRange],groupId:String) ={
      DB.localTx({
        implicit session =>
          for (offsetRange <- offsetRanges) {
            //LOG.info("--------偏移量------"+offsetRange.untilOffset.toLong+"-------------")
            //replace是有的话直接删除，再插入，update则是直接插入
          SQL("replace into bigdata_kafkaoffset (groupid,topic,kafka_partition,kafka_offset) values(?,?,?,?)").bind(groupId.toString, offsetRange.topic.toString, offsetRange.partition.toString, offsetRange.untilOffset.toLong).update().apply()
          }
      })
  }
}
