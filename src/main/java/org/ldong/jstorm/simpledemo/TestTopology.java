package org.ldong.jstorm.simpledemo;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import org.ldong.jstorm.simpledemo.bolt.TestBolt;
import org.ldong.jstorm.simpledemo.spolt.TestSpout;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Properties;

@Component("testtopology")
public class TestTopology implements ILogTopology {
    public static void main(String[] args){
        Config cf = new Config();
        TestTopology ty = new TestTopology();
        try {
            ty.start(new Properties());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void start(Properties properties) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, IOException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("testspout", new TestSpout(), 1);
        builder.setBolt("testbolt", new TestBolt(), 2).shuffleGrouping("testspout");

        Config conf = ConfigUtils.getStormConfig(properties);
        conf.setNumAckers(1);

        StormSubmitter.submitTopology("testtopology", conf, builder.createTopology());
        System.out.println("storm cluster will start");
    }

}
