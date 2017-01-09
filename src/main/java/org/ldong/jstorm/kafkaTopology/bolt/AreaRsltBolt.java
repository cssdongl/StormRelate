package org.ldong.jstorm.kafkaTopology.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.ldong.jstorm.hbase.HBaseDAO;
import org.ldong.jstorm.hbase.HBaseDAOImp;

import java.util.HashMap;
import java.util.Map;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/1/7 14:03
 */
public class AreaRsltBolt implements IBasicBolt {

    private static final long serialVersionUID = 1L;
    Map <String,Double> countsMap = null ;
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        dao = new HBaseDAOImp() ;
        countsMap = new HashMap<String, Double>() ;
    }

    HBaseDAO dao = null;
    long beginTime = System.currentTimeMillis() ;
    long endTime = 0L ;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("we reach here to write to hbase");
        String date_areaid = input.getString(0);
        double order_amt = input.getDouble(1) ;
        countsMap.put(date_areaid, order_amt) ;
        endTime = System.currentTimeMillis() ;
        if (endTime - beginTime >= 5 * 1000) {
            for(String key : countsMap.keySet())
            {
                // put into hbase
                dao.insert("storm_hbase", key, "info", "order_amt", countsMap.get(key)+"") ;
                System.err.println("rsltBolt put hbase: key="+key+"; order_amt="+countsMap.get(key));
            }
        }
    }

    @Override
    public void cleanup() {

    }
}
