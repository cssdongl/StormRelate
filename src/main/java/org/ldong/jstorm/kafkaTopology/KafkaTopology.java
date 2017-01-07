package org.ldong.jstorm.kafkaTopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.ldong.jstorm.kafka.constants.KafkaProperties;
import org.ldong.jstorm.kafkaTopology.bolt.AreaAmtBolt;
import org.ldong.jstorm.kafkaTopology.bolt.AreaFilterBolt;
import org.ldong.jstorm.kafkaTopology.bolt.AreaRsltBolt;
import org.ldong.jstorm.kafkaTopology.spolt.OrderBaseSpout;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/1/7 14:02
 */
public class KafkaTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new OrderBaseSpout(KafkaProperties.ORDER_TOPIC), 5);
        builder.setBolt("filterblot", new AreaFilterBolt() , 5).shuffleGrouping("spout") ;
        builder.setBolt("amtbolt", new AreaAmtBolt() , 2).fieldsGrouping("filterblot", new Fields("area_id")) ;
        builder.setBolt("rsltolt", new AreaRsltBolt(), 1).shuffleGrouping("amtbolt");


        Config conf = new Config() ;
        conf.setDebug(false);
        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }else {
            //本地测试！！！！！！！！！！！！
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("mytopology", conf, builder.createTopology());
        }


    }
}
