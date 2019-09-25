package com.wisdom.hbase;

import com.wisdom.utils.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by BaldKiller
 * on 2019/9/2 16:21
 */
public class HbaseAPI {
    public static void main(String[] args) throws IOException {
        System.out.println(HBaseUtil.isTableExist("student"));
        HBaseUtil.createTable("student","info");
    }


}
