package com.hbase24;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class HbaseClientDML {
    Connection conn= null;
    @Before
    public void getConn() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.11.197:2181,192.168.11.196:2181,192.168.11.195:2181,192.168.11.194:2181");
        conn= ConnectionFactory.createConnection(conf);
    }

    /*
    * 增
    * */
    @Test
    public void testPut() throws IOException {
        Table table=conn.getTable(TableName.valueOf("user_info"));
        List<Put> puts = new ArrayList<>();
        Put put = new Put(Bytes.toBytes("005"));
        put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("play"),Bytes.toBytes("game"));
        put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("aihao"),Bytes.toBytes("read"));
        puts.add(put);

        Put put2 = new Put(Bytes.toBytes("004"));
        put2.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("play"),Bytes.toBytes("game"));
        put2.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("aihao"),Bytes.toBytes("read"));
        puts.add(put2);

        table.put(puts);
        table.close();
        conn.close();
    }

    @Test
    public void testManyPut() throws IOException {
        Table table=conn.getTable(TableName.valueOf("user_info"));
        List<Put> puts = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Put put = new Put(Bytes.toBytes(""+i));
            put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("play"),Bytes.toBytes("game"));
            put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("age"),Bytes.toBytes(""+new Random().nextInt(60)));
            put.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("username"),Bytes.toBytes("wang "+new Random().nextInt(60)));
            puts.add(put);
        }


        table.put(puts);
        table.close();
        conn.close();
    }


    /*
     * 删
     * */
    @Test
    public void testDelete() throws IOException {
        Table table=conn.getTable(TableName.valueOf("user_info"));

        Delete delete = new Delete(Bytes.toBytes("005"));
        delete.addColumn(Bytes.toBytes("base_info"),Bytes.toBytes("aihao"));
        table.delete(delete);
        table.close();
        conn.close();
    }

    /*
     * 改
     * */
    @Test
    public void testChange() throws IOException {
        Table table=conn.getTable(TableName.valueOf("user_info"));

        Delete delete = new Delete(Bytes.toBytes("001"));
        table.delete(delete);
    }

    /*
     * 查
     * */
    @Test
    public void testFind() throws IOException {
        Table table=conn.getTable(TableName.valueOf("user_info"));
        Get get = new Get(Bytes.toBytes("001"));

        Result result =table.get(get);
        for (Cell cell:result.rawCells())
        {
            String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println("colName="+colName+",value="+value);
        }
        table.close();
        conn.close();
    }

    @Test
    public void testScan() throws IOException {
        Table table=conn.getTable(TableName.valueOf("user_info"));
        Scan scan = new Scan("001".getBytes(),"1000\000".getBytes());
        ResultScanner resultScanner=table.getScanner(scan);
        Iterator<Result> iterator = resultScanner.iterator();
        while (iterator.hasNext())
        {
            Result result = iterator.next();
            for (Cell cell:result.rawCells())
            {
                String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("colName="+colName+",value="+value);
            }

        }

        table.close();
        conn.close();
    }
}
