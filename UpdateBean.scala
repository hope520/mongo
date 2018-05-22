package cn.fulin

/**
  * Created by Administrator on 2018/4/11.
  */


//样例类 -封装修改的用户信息
case class UpdateBean(
                       mobile: String, //手机号
                       overdueAmt: String, //罚滞金额
                       overdueDays: String, //逾期天数
                       overdueId: String, //唯一标识
                       overdueStatus: String, //逾期状态
                       overdueTotalAmt: String, //逾期总额=本金+利息+滞纳金
                       seqNo: String, //批次号
                       status: String, //其他备注，状态信息
                       lastRepayTime: String ,//最近一次还款时间
                       lastRepayAmt: String //最近一次还款金额
                     )
