package cn.fulin

import cn.fulin.OverdueNewAdd2Mysql2.getFullUserInfo
import cn.fulin.WriteSql.LOG
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scalikejdbc.{DB, NamedDB, SQL}
import test.GetJsonData

/**
  * Created by Administrator on 2018/5/11.
  */
object InsertAddAndPnum {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("InsertAddAndPnum")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)


    val lines: RDD[String] = sc.textFile("E:\\30.txt")

    val addressRdd: RDD[(String, String)] = lines.map(line => {

      val tp: (String, String) = getData4Mysql(line)
      val num: String = tp._2
      val userInfo: String = GetUserInfoFromHBase.getHbasedata(num, "MOXIE_CARRIER")
      println(userInfo)

      //得到用户联系频次最大的top10和户籍地址
      val json: JSONObject = JSON.parseObject(userInfo)

      if (null == json) {

        val address = "未知"
        (tp._1, address)
      } else {
        val obj = json.getJSONObject("mxReport")
        println(obj)

        if (null == obj) {

          val address = "未知"
          (tp._1, address)
        } else {

          //得到用户户籍地址
          val arr2: JSONArray = obj.getJSONArray("cell_phone")
          val address = GetJsonData.getAddress(arr2)

          (tp._1, address)
        }
      }
    })


    addressRdd.foreach(tp=>{
      getAddByPhone(tp)
    })

    sc.stop()
  }


  //更新Update表中相应字段
  def getData4Mysql(line: String): (String, String) = {
    LOG.info(s"-----$line-----")
    //if (line != null) {
    val tuples: List[(String, String)] = DB.readOnly {
      implicit session =>
        SQL(
          """
SELECT overdue_id,phone FROM detail_overdue_days_report WHERE overdue_id = ?
            """.stripMargin).bind(line)
          .map(t => {
            (t.string("overdue_id"), t.string("phone"))
          }).toList().apply()
    }
    tuples(0)

    // }

  }


  //更新Update表中相应字段
  def writeData2Mysql4Update(tp: (String, String, String)) = {
    LOG.info(s"-----$tp-----")

    DB.localTx({
      implicit session =>
        SQL(
          """
            |UPDATE detail_overdue_days_report SET
            |address_detail = ?
            |WHERE overdue_id = ?
          """.stripMargin).bind(
          tp._2,
          tp._3,
          tp._1
        ).update().apply()
    })
  }


  //更新Update表中相应字段
  def getAddByPhone(tp: (String, String)) = {
    LOG.info(s"-----$tp-----")

    DB.localTx({
      implicit session =>
        SQL(
          """
            |UPDATE detail_overdue_days_report SET
            |address_detail = ?
            |WHERE overdue_id = ?
          """.stripMargin).bind(
          tp._2,
          tp._1
        ).update().apply()
    })
  }



}
