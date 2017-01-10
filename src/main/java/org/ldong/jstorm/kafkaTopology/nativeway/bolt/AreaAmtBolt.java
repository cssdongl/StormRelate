package org.ldong.jstorm.kafkaTopology.nativeway.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.ldong.jstorm.hbase.HBaseDAO;
import org.ldong.jstorm.hbase.HBaseDAOImp;
import org.ldong.jstorm.kafka.simpletopic.DateFormat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/1/7 14:05
 */
public class AreaAmtBolt implements IBasicBolt {
    private static final long serialVersionUID = 1L;
    Map <String,Double> countsMap = null ;
    String today = null;
    HBaseDAO dao = null;

    @Override
    public void cleanup() {
        //???
        countsMap.clear() ;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date_area","amt")) ;

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        countsMap = new HashMap<String, Double>() ;
        dao = new HBaseDAOImp() ;
        //根据hbase里初始值进行初始化countsMap
        today = DateFormat.getCountDate(null, DateFormat.date_short);
        countsMap = this.initMap(today, dao);
        for(String key:countsMap.keySet())
        {
            System.err.println("key:"+key+"; value:"+countsMap.get(key));
        }
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input != null) {
            String area_id = input.getString(0) ;
            double order_amt = 0.0;
            //order_amt = input.getDouble(1) ;
            try {
                order_amt = Double.parseDouble(input.getString(1)) ;
            } catch (Exception e) {
                System.out.println(input.getString(1)+":---------------------------------");
                e.printStackTrace() ;
            }

            String order_date = input.getStringByField("order_date") ;

            if (! order_date.equals(today)) {
                //跨天处理
                countsMap.clear() ;
            }

            Double count = countsMap.get(order_date+"_"+area_id) ;
            if (count == null) {
                count = 0.0 ;
            }
            count += order_amt ;
            countsMap.put(order_date+"_"+area_id, count) ;
            System.err.println("areaAmtBolt:"+order_date+"_"+area_id+"="+count);
            collector.emit(new Values(order_date+"_"+area_id,count)) ;
            System.out.println("***********"+order_date+"_"+area_id+count);
        }

    }

    public Map<String, Double> initMap(String rowKeyDate, HBaseDAO dao)
    {
        Map <String,Double> countsMap = new HashMap<String, Double>() ;
        List<Result> list = dao.getRows("storm_hbase", rowKeyDate, new String[]{"order_amt"});
        for(Result rsResult : list)
        {
            String rowKey = new String(rsResult.getRow());
            for(KeyValue keyValue : rsResult.raw())
            {
                if("order_amt".equals(new String(keyValue.getQualifier())))
                {
                    countsMap.put(rowKey, Double.parseDouble(new String(keyValue.getValue()))) ;
                    break;
                }
            }
        }

        return countsMap;
    }
}
