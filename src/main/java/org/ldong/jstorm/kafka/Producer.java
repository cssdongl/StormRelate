package org.ldong.jstorm.kafka;

import backtype.storm.utils.Utils;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.ldong.jstorm.kafka.constants.KafkaProperties;

import java.util.Properties;
import java.util.Random;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/1/7 13:36
 */
public class Producer extends Thread {
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public Producer(String topic) {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "192.168.15.81:9092");
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }

    public void run() {
        // order_id,order_amt,create_time,area_id
        Random random = new Random();
        String[] order_amt = {"10.10", "20.10", "30.10", "40.0", "60.10"};
        String[] area_id = {"1", "2", "3", "4", "5"};

        int i = 0;
        while (true) {
            i++;
            String messageStr = i + "\t" + order_amt[random.nextInt(5)] + "\t" +
                    DateFmt.getCountDate(null, DateFmt.date_long) + "\t" + area_id[random.nextInt(5)];
            System.out.println("product:" + messageStr);
            producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
            Utils.sleep(1000) ;
        }
    }

    public static void main(String[] args) {
        Producer producerThread = new Producer(KafkaProperties.STROM_TOPIC);
        producerThread.start();
    }
}
