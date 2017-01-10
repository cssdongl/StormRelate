package org.ldong.jstorm.kafka.jsontopic;

import backtype.storm.utils.Utils;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.ldong.jstorm.kafka.constants.KafkaProperties;
import org.ldong.jstorm.kafka.simpletopic.DateFormat;

import java.util.Properties;
import java.util.Random;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/1/7 13:36
 */
public class ComplexJsonProducer extends Thread {
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    private String[] devices = {
            "ad4e2143de0048edb699eb53216f3e04","1de4f248eed9f9678c838cd78cae4da0",
            "b5294a30a79b76344e1da976f3b527ed","28c15697e509f6657fecd0527517ad3e",
            "753d3719bad01be002aba73b8f2672bc","2757898f2c83a5e961b2104e882fcaae",
            "2757898f2c83a5e961b2104e882fcaae","6b67c8c700427dee7552f81f3228c927"};

    public ComplexJsonProducer(String topic) {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "192.168.15.81:9092");
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }

    public void run() {
        Random random = new Random();
        String os = "Android";
        String osVersion = "";

        String[] order_amt = {"10.10", "20.10", "30.10", "40.0", "60.10"};
        String[] area_id = {"1", "2", "3", "4", "5"};

        int i = 0;
        while (true) {
            i++;
            String messageStr = i + "\t" + order_amt[random.nextInt(5)] + "\t" +
                    DateFormat.getCountDate(null, DateFormat.date_long) + "\t" + area_id[random.nextInt(5)];
            System.out.println("product:" + messageStr);
            producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
            Utils.sleep(5000) ;
        }
    }

    public static void main(String[] args) {
        ComplexJsonProducer producerThread = new ComplexJsonProducer(KafkaProperties.STROM_TOPIC);
        producerThread.start();
    }
}
