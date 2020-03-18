package cn.com.hbase.api;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HbaseConn {

    static Configuration configuration = null;

    static Connection connection = null;

    /**
     * 初始化配置信息：zookeeper信息
     */
    public HbaseConn() {

        if (configuration == null) {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.rootdir", "hdfs://hadoop100:8020/hbase");
            configuration.set("hbase.zookeeper.quorum", "hadoop100:2181");

        }

    }

    private static final class InnerHC {
        private static final HbaseConn instance = new HbaseConn();
    }

    /**
     * 获取客户端连接
     */
    private Connection getConnection() {
        try {

            if (connection == null || connection.isClosed()) {

                connection = ConnectionFactory.createConnection(configuration);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return connection;
    }


    /**
     * 获取hbase 客户端连接
     *
     * @return
     */
    public static Connection getHBaseConn() {
        return InnerHC.instance.getConnection();
    }


    /**
     * 获取表实例
     *
     * @param tableName
     * @return
     */
    public static Table getTable(String tableName) {
        Table table = null;
        try {
            table = InnerHC.instance.getConnection().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    /**
     * 关闭连接
     */
    public static void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
