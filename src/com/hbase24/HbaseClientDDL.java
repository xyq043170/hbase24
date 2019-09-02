package com.hbase24;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HbaseClientDDL {
    Connection conn= null;
    @Before
    public void getConn() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.11.197:2181,192.168.11.196:2181,192.168.11.195:2181,192.168.11.194:2181");
        conn=ConnectionFactory.createConnection(conf);
    }

    @Test
    public void testCreateTable() throws IOException {


        Admin admin=conn.getAdmin();
        //表名
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("user_info"));
        //列族名
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("base_info");
        hColumnDescriptor.setMaxVersions(3);
        HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("extra_info");
        //添加列族
        hTableDescriptor.addFamily(hColumnDescriptor);
        hTableDescriptor.addFamily(hColumnDescriptor2);
        admin.createTable(hTableDescriptor);

        admin.close();
        conn.close();
    }

    @Test
    public void testDropTable() throws IOException {
        Admin admin=conn.getAdmin();

        admin.disableTable(TableName.valueOf("user_info"));
        admin.deleteTable(TableName.valueOf("user_info"));

        admin.close();
        conn.close();
    }

    @Test
    public void testAlterTable() throws Exception
    {
        Admin admin=conn.getAdmin();
        HTableDescriptor hTableDescriptor=admin.getTableDescriptor(TableName.valueOf("user_info"));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("other_info2");
        hColumnDescriptor.setBloomFilterType(BloomType.ROWCOL);
        hTableDescriptor.addFamily(hColumnDescriptor);

        admin.modifyTable(TableName.valueOf("user_info"),hTableDescriptor);

        admin.close();
        conn.close();
    }
}
