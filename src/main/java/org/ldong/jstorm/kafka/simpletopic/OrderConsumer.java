package org.ldong.jstorm.kafka.simpletopic;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.ldong.jstorm.kafka.constants.KafkaProperties;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/1/7 13:57
 */
public class OrderConsumer extends Thread{

    private final ConsumerConnector consumer;
    private final String topic;

    private Queue<String> queue = new ConcurrentLinkedQueue<String>() ;//有序队列

    public OrderConsumer(String topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
        this.topic = topic;
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", KafkaProperties.ZK_CONNECT);
        props.put("group.id", KafkaProperties.GROUP_ID);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");//zookeeper offset偏移量

        return new ConsumerConfig(props);

    }
    // push消费方式，服务端推送过来。主动方式是pull
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext()){
            //逻辑处理
            System.out.println("consumer:"+new String(it.next().message()));
            queue.add(new String(it.next().message())) ;
            System.err.println("队列----->"+queue);
        }

    }

    public Queue<String> getQueue()
    {
        return queue ;
    }

    public static void main(String[] args) {
        OrderConsumer consumerThread = new OrderConsumer(KafkaProperties.STROM_TOPIC);
        consumerThread.start();

    }
}
