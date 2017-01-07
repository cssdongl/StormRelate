package org.ldong.jstorm.hbase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;
/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/1/7 13:43
 */
public interface HBaseDAO {
     void save(Put put,String tableName) ;
     void insert(String tableName,String rowKey,String family,String quailifer,String value) ;
     void save(List<Put>Put ,String tableName) ;
     Result getOneRow(String tableName,String rowKey) ;
     List<Result> getRows(String tableName,String rowKey_like) ;
     List<Result> getRows(String tableName, String rowKeyLike, String cols[]) ;
     List<Result> getRows(String tableName,String startRow,String stopRow) ;
}
