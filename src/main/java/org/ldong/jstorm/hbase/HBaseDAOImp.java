package org.ldong.jstorm.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/1/7 13:46
 */
public class HBaseDAOImp implements HBaseDAO {

    private static Configuration conf;
    private static Connection conn = null;
    static {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.15.81,192.168.15.82,192.168.15.83");
        conf.set("hbase.rootdir", "hdfs://192.168.15.84:8020/hbase");
//        conf = HBaseConfiguration.create();
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            conn = null;
            e.printStackTrace();
        }
    }

    public HBaseDAOImp() {

    }

    @Override
    public void save(Put put, String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            table.put(put);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void insert(String tableName, String rowKey, String family,
                       String quailifer, String value) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowKey.getBytes());
            put.addColumn(family.getBytes(), quailifer.getBytes(), value.getBytes());
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void save(List<Put> Put, String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            table.put(Put);
        } catch (Exception e) {
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


    @Override
    public Result getOneRow(String tableName, String rowKey) {
        Table table = null;
        Result rsResult = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            rsResult = table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rsResult;
    }

    @Override
    public List<Result> getRows(String tableName, String rowKeyLike) {
        Table table = null;
        List<Result> list = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rs : scanner) {
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public List<Result> getRows(String tableName, String rowKeyLike, String cols[]) {
        Table table = null;
        List<Result> list = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
            Scan scan = new Scan();
            for (int i = 0; i < cols.length; i++) {
                scan.addColumn("info".getBytes(), cols[i].getBytes());
            }
            scan.setFilter(filter);
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rs : scanner) {
                list.add(rs);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public List<Result> getRows(String tableName, String startRow, String stopRow) {
        Table table = null;
        List<Result> list = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(stopRow.getBytes());
            ResultScanner scanner = table.getScanner(scan);
            list = new ArrayList<Result>();
            for (Result rsResult : scanner) {
                list.add(rsResult);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public static void main(String[] args) {
        HBaseDAO dao = new HBaseDAOImp();
        List<Put> list = new ArrayList<Put>();
        Put put = new Put("aa".getBytes());
        dao.insert("storm_hbase","aa","info","age","33");
        put.addColumn("info".getBytes(), "name".getBytes(), "zhangsan".getBytes());
        list.add(put);
        put.addColumn("info".getBytes(), "addr".getBytes(), "beijing".getBytes());
        list.add(put);
        put.addColumn("info".getBytes(), "age".getBytes(), "30".getBytes());
        list.add(put);
        put.addColumn("info".getBytes(), "tel".getBytes(), "13567882341".getBytes());
        list.add(put);
        dao.save(list, "storm_hbase");

    }

}
