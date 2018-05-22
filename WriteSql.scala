package cn.fulin

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.logging.LogFactory
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
  * Created by 13332 on 2018/4/19.
  */
object WriteSql {
  var LOG = LogFactory.getLog(WriteSql.getClass)
  //加载配置信息将指标储入RDS数据库
  DBs.setup()

  //用于写入数据库的方法
  def writeDataInMysql(bean: NewBean) = {
    LOG.info(s"-----$bean-----")
    if (bean.mobile != null) {
      DB.localTx({
        implicit session =>
          SQL(
            """|replace into detail_overdue_days_report (overdue_id,name,
               |card_id,phone,org_name,contract_no,age,sex,email,qq,
               |wechat,ali_id,real_amt,overdue_total_amt,entrust_amt,
               |overdue_days,address,address_detail,company_address,
               |permanent_address,bank_card,company_name,loan_date,stages,
               |loan_amt,repay_amt,balance,product_name,overdue_amt,overdue_status,
               |last_repay_time,repay_num,home_phone,company_phone,contacts,
               |contacts_type,contacts_phone,contacts_remark,contacts1,contacts1_type,
               |contacts1_phone,contacts1_remark,remark,overdue_date,created_date,
               |updated_date,entrust_start_date,entrust_end_date,seq_no,last_repay_amt,
               |allot_id,call_record_top_ten)
               |values
               |(?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?)
            """.stripMargin).bind(
            bean.overdueId,//唯一标识
            bean.name,//姓名
            bean.cardId,//身份证号
            bean.mobile,//手机号
            bean.orgName,//贷款机构
            bean.contracNo,//用户号
            bean.age,//年龄
            bean.sex,//性别
            bean.email,//邮箱
            bean.qq,//QQ号码
            bean.weChat,//微信
            bean.aliId,//支付宝
            bean.realAmt,//放款金额
            bean.overdueTotalAmt,//逾期总额=本金+利息+滞纳金
            bean.entrustAmt,//委托金额第一天的逾期总额
            bean.overdueDays,//逾期天数
            bean.address2,//案件地区
            bean.address,//户籍地址
            bean.companyAddress,//公司地址
            bean.permanentAddress,//常住地址
            bean.bankCard,//银行卡
            bean.companyName,//客户所属公司
            bean.loanDate,//放款日期
            bean.stages,//借款期数
            bean.loanAmt,//签约金额
            bean.repayAmt,//月还款金额
            bean.balance,//剩余本金
            bean.productName,//借款产品名称
            bean.overdueAmt,//罚滞金额
            bean.overdueStatus,//逾期状态
            bean.lastRepayTime,//最近一次还款时间
            bean.repayNum,//已还期数
            bean.homePhone,//家庭电话
            bean.companyPhone,//公司电话
            bean.contacts,//联系人1
            bean.contactsType,//联系人类型1
            bean.contactsPhone,//联系人电话1
            bean.contactsRemark,//联系人备注1
            bean.contacts2,//联系人2
            bean.contactsType2,//联系人类型2
            bean.contactsPhone2,//联系人电话2
            bean.contactsRemark2,//联系人备注2
            bean.status,//其他备注
            bean.overdueDate,//逾期日期格式,逾期日期开始时间
            bean.createdDate,//创建日期
            bean.updatedDate,//修改日期
            bean.entrustStartDate,//委托起始时间Total
            bean.entrustEndDate,//委托结束时间
            bean.seqNo,//批次号
            bean.lastRepayAmt,//最近一次还款金额
        bean.allotId,//分配员

        bean.callRecordTopTen//通讯录top10

            ).update().apply()
      })
    }
  }



  //更新Update表中相应字段
  def writeData2MysqlUpdate(bean: UpdateBean) = {
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
               |overdue_amt = ?,
               |overdue_days = ?,
               |overdue_status = ?,
               |overdue_total_amt = ?,
               |seq_no = ?,
               |remark = ?,
               |loan_type = ?,
               |updated_date = ?
               |WHERE overdue_id = ?
            """.stripMargin).bind(
            bean.lastRepayAmt,
            bean.lastRepayTime,
            bean.mobile,
            bean.overdueAmt,
            bean.overdueDays,
            bean.overdueStatus,
            bean.overdueTotalAmt,
            bean.seqNo,//批次号
            bean.status,
            "信贷",
            date,
            bean.overdueId

          ).update().apply()
      })
    }
  }

  //用于写入数据库的方法
  def writeHisData2Mysql(bean: NewBean) = {

    LOG.info(s"-----$bean-----")
    if (bean.mobile != null) {
      DB.localTx({
        implicit session =>
          SQL(
            """|replace into overdue_update (overdueId,seqNo,name,age,sex,cardId,
               |address2,address,companyAddress,permanentAddress,mobile,orgName,
               |loanDate,stages,loanAmt,realAmt,repayAmt,balance,overdueAmt,
               |overdueTotalAmt,overdueStatus,lastRepayTime,lastRepayAmt,loanType,
               |overdueDays,entrustAmt,collectionType,repayNum,billDate,productName,
               |bankCard,companyName,interestRate,entrustStartDate,entrustEndDate,
               |homePhone,companyPhone,qq,weChat,email,aliId,startSeatNo,status)
               |values
               |(?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?,
               |?)
            """.stripMargin).bind(
            //当前日期
            bean.overdueId,//唯一标识
            bean.seqNo,//批次号
            bean.name,//姓名
            bean.age,//年龄
            bean.sex,//性别
            bean.cardId,//身份证号
            bean.address2,//案件地区
            "",//户籍地址
            bean.companyAddress,//公司地址
            bean.permanentAddress,//常住地址
            bean.mobile,//手机号
            bean.orgName,//贷款机构
            bean.loanDate,//放款日期
            bean.stages,//借款期数
            bean.loanAmt,//签约金额
            bean.realAmt,//放款金额
            bean.repayAmt,//月还款金额
            bean.balance,//剩余本金
            bean.overdueAmt,//罚滞金额
            bean.overdueTotalAmt,//逾期总额=本金+利息+滞纳金
            bean.overdueStatus,//逾期状态
            bean.lastRepayTime,//最近一次还款时间
            bean.lastRepayAmt,//最近一次还款金额
            bean.loanType,//贷款类型
              bean.overdueDays,//逾期天数
        bean.entrustAmt,//委托金额第一天的逾期总额
        bean.collectionType,//催收类型
        bean.repayNum,//已还期数
        bean.billDate,//账单日
        bean.productName,//借款产品名称
        bean.bankCard,//银行卡
        bean.companyName,//客户所属公司
        bean.interestRate,//佣金利率
        bean.entrustStartDate,//委托起始时间Total
        bean.entrustEndDate,//委托结束时间
        bean.homePhone,//家庭电话
        bean.companyPhone,//公司电话
        bean.qq,//QQ号码
        bean.weChat,//微信
        bean.email,//邮箱
        bean.aliId,//支付宝
        bean.startSeatNo,//初始坐席号
            bean.status

          ).update().apply()
      })
    }
  }



}
