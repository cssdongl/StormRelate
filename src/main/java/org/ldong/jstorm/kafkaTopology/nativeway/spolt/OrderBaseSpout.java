package org.ldong.jstorm.kafkaTopology.nativeway.spolt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.ldong.jstorm.kafka.simpletopic.OrderConsumer;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/1/7 14:07
 */
public class OrderBaseSpout implements IRichSpout {
    String topic = null;
    public OrderBaseSpout(String topic)
    {
        this.topic = topic ;
    }
    /**
     * 公共基类spout
     */
    private static final long serialVersionUID = 1L;
    Integer TaskId = null;
    SpoutOutputCollector collector = null;
    Queue<String> queue = new ConcurrentLinkedQueue<String>() ;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("order")) ;
    }

    public void nextTuple() {
        if (queue.size() > 0) {
            String str = queue.poll() ;
            //进行数据过滤
            System.err.println("TaskId:"+TaskId+";  str="+str);
            collector.emit(new Values(str)) ;
        }
    }

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector ;
        TaskId = context.getThisTaskId() ;
//        Thread.currentThread().getId()
        OrderConsumer consumer = new OrderConsumer(topic) ;
        consumer.start() ;
        queue = consumer.getQueue() ;
    }


    public void ack(Object msgId) {
        // TODO Auto-generated method stub

    }


    public void activate() {
        // TODO Auto-generated method stub

    }


    public void close() {
        // TODO Auto-generated method stub

    }


    public void deactivate() {
        // TODO Auto-generated method stub

    }


    public void fail(Object msgId) {
        // TODO Auto-generated method stub

    }


    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
