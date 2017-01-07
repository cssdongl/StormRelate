package org.ldong.jstorm.kafkaTopology.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.ldong.jstorm.kafka.DateFmt;

import java.util.Map;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/1/7 14:06
 */
public class AreaFilterBolt implements IBasicBolt{
    private static final long serialVersionUID = 1L;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("area_id","order_amt","order_date"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        // TODO Auto-generated method stub

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String order = input.getString(0);
        if(order != null){
            String[] orderArr = order.split("\\t");
            // ared_id,order_amt,create_time
            collector.emit(new Values(orderArr[3],orderArr[1], DateFmt.getCountDate(orderArr[2], DateFmt.date_short)));
            System.out.println("--------------》"+orderArr[3]+orderArr[1]);
        }

    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }
}
