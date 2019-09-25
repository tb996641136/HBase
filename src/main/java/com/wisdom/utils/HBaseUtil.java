package com.wisdom.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by BaldKiller
 * on 2019/9/3 13:59
 */
public class HBaseUtil {
    // 获取HBase配置
    public static Configuration conf;

    static {
        System.setProperty("java.security.krb5.conf", "C:\\Users\\tianbo\\Desktop\\krb5.conf");
        conf = HBaseConfiguration.create();
        conf.set("hadoop.security.authentication", "Kerberos" );
        UserGroupInformation. setConfiguration(conf);
        try {
            //UserGroupInformation.loginUserFromKeytab("hbase_admin@FAYSON.COM", "C:\\Users\\tianbo\\Desktop\\hbase_admin.keytab");
            UserGroupInformation.loginUserFromKeytab("cloudera-test@FAYSON.COM", "C:\\Users\\tianbo\\Desktop\\cloudera-test.keytab");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /*
     *   判断表是否存在
     * */
    public static boolean isTableExist(String tableName) throws IOException {
        // 获取HBase连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        // 获取HBase操作对象admin
        Admin admin = conn.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /*
     *   创建表
     * */
    public static void createTable(String tableName, String... columnFamilys) throws IOException {
        // 获取HBase连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        // 获取HBase操作对象admin
        Admin admin = conn.getAdmin();

        // 先判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表已存在");
        } else {
            // 创建表描述对象 表描述作用就是添加表明以及列族
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String columnFanily : columnFamilys) {
                descriptor.addFamily(new HColumnDescriptor(columnFanily));
            }

            admin.createTable(descriptor);
            System.out.println("表创建成功...");
        }
    }

    /*
     *   删除表
     * */
    public static void dropTable(String tableName) throws IOException {
        // 获取HBase连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        // 获取HBase操作对象admin
        Admin admin = conn.getAdmin();

        // 判断表是否存在
        if (isTableExist(tableName)) {
            // 删除表之前需要先将表不可用，然后才能删表
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("表 : " + tableName + "删除成功！");
        } else {
            System.out.println("表 : " + tableName + "不存在！");

        }
    }

    /*
     *   向表中插入数据
     * */
    public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String values) throws IOException {
        // 获取HBase连接对象
        Connection conn = ConnectionFactory.createConnection(conf);

        Table table = conn.getTable(TableName.valueOf(tableName));
        //创建Put对象 向里面塞数据
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(values));

        table.put(put);
        table.close();
        System.out.println("数据插入成功！");
    }

    /*
     *   删除多行数据
     * */
    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        // 获取HBase连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));

        List<Delete> deletes = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deletes.add(delete);
        }
        table.delete(deletes);
        table.close();
        System.out.println("数据删除成功...");
    }

    /*
    *   获取所有数据
    * */
    public static void getAllRow(String tableName) throws IOException {
        // 获取HBase连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));

        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /*
    *   获取一行数据
    * */
    public static void getRow(String tableName, String rowKey) throws IOException{
        // 获取HBase连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

    /*
    *   获取某一行指定“列族:列”的数据
    *
    * */
    public static void getRowQualifier(String tableName, String rowKey, String family, String
            qualifier) throws IOException{
        // 获取HBase连接对象
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

}
