package cn.fulin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by leibiao on 2017/3/6.
 */
public class GetUserInfoFromHBase {

    private static final String TABLE_NAME = "RISK"; //表名
    private static final String CF_DEFAULT = "RISK_REPORT"; //列族
     //public static final String COLNAME = "SDK_COLLECTION_INFO"; //列名
    //public static final String COLNAME = "MOXIE_CARRIER"; //列名
    //public static final String COLNAME = "MOXIE_CARRIER_SPEC"; //列名


//   public static void main(String[] args) {
//       String sdk = getHbasedata("18396019327","SDK_COLLECTION_INFO");
//       System.out.println(sdk);
//   }


    public static String getHbasedata(String rowkey,String colname) {
        Configuration config = HBaseConfiguration.create();
        //String zkAddress = "hb-uf6t3d1id8a781ds9-001.hbase.rds.aliyuncs.com:2181,hb-uf6t3d1id8a781ds9-002.hbase.rds.aliyuncs.com:2181,hb-uf6t3d1id8a781ds9-003.hbase.rds.aliyuncs.com:2181";
        //外网
        String zkAddress = "hb-proxy-pub-uf6t3d1id8a781ds9-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-uf6t3d1id8a781ds9-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-uf6t3d1id8a781ds9-003.hbase.rds.aliyuncs.com:2181";
        config.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
        Connection connection = null;
        String res = "";

        try {
            connection = ConnectionFactory.createConnection(config);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT));
            Admin admin = connection.getAdmin();

            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            String tr = get(table, rowkey, config,colname);
            if(tr != null && tr != ""){
                res = tr;
            };
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return res;
    }


    public static String get(Table table, String rowkey, Configuration config,String colname) throws IOException {

        byte[] ROWKEY = Bytes.toBytes(rowkey);
        Get get = new Get(ROWKEY);
        Result rs = table.get(get);
        //System.out.println(" 表获取数据成功！");
        byte[] b = rs.getValue(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(colname));
        //System.out.println(Bytes.toString(b));
        String s = Bytes.toString(b);
        return s;
    }
}
