package cn.fulin

/**
  * Created by Administrator on 2018/4/11.
  */


//样例类 -封装新增用户信息
case class NewBean(

                    age: String,
                    balance: String,
                    bankCard: String,
                    cardId: String,
                    contacts: String,
                    contactsPhone: String,
                    contactsRemark: String,
                    contactsType: String,
                    entrustAmt: String,
                    entrustEndDate: String,
                    entrustStartDate: String,
                    lastRepayTime: String,
                    loanAmt: String,
                    loanDate: String,
                    mobile: String,
                    name: String,
                    orgName: String,
                    overdueAmt: String,
                    overdueDate: String,
                    overdueDays: String,
                    overdueId: String,
                    overdueStatus: String,
                    overdueTotalAmt: String,
                    productName: String,
                    realAmt: String,
                    sex: String,
                    stages: String,
                    status: String,//其他备注，状态信息

                    address: String,
                    callRecordTopTen: String,//通话次数最多的十个人//
                    seqNo:String,

                    address2:String, //案件地区
                    companyAddress:String,//公司地址
                    permanentAddress:String,//常住地址
                    companyName:String,//客户所属公司
                    repayAmt:String,//月还款金额
                    lastRepayAmt:String,//最近一次还款金额
                    repayNum:String,//已还期数
                    homePhone:String,//家庭电话
                    companyPhone:String,//公司电话
                    qq:String,//QQ号码
                    weChat:String,//微信
                    email:String,//邮箱
                    aliId:String,//支付宝
                    contacts2:String,//联系人2
                    contactsType2:String,//联系人类型2
                    contactsPhone2:String,//联系人电话2
                    contactsRemark2:String,//联系人备注2
                    allotId:String,//分配员

                      loanType:String,//贷款类型
                      collectionType:String,//催收类型
                      billDate:String,//账单日
                      interestRate:String,//佣金利率
                    startSeatNo:String,//初始坐席号
                    createdDate:String,//创建日期
                    updatedDate:String,//修改日期
                    contracNo:String//用户号





                  )
