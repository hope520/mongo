package cn.fulin

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by Administrator on 2018/5/11.
  */
object GetDataFromMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()








    val lines: RDD[String] = spark.sparkContext.textFile("E:\\21.txt")
    lines.foreach(line=>{
      println(line)
      val url = "jdbc:mysql://47.97.179.224:3306/bigdatatest?characterEncoding=utf-8"
      val table = "overdue_increment"
      val prop = new Properties()
      prop.setProperty("user","root")
      prop.setProperty("password","1017~Fulin")
      prop.setProperty("driver","com.mysql.jdbc.Driver")
      val df: DataFrame = spark.read.jdbc(url,table,prop)
      df.createOrReplaceTempView("v_overdue")
      var sql="SELECT overdueId,address, callRecordTopTen FROM v_overdue WHERE overdueId ="+line
      println(sql)

      val filtered: DataFrame = spark.sql(sql)

      filtered.repartition(1).write.mode(SaveMode.Append).text("E:\\overdue")

    })



spark.stop()
  }


}
