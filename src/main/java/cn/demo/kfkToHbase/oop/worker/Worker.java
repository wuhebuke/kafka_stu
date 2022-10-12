package cn.demo.kfkToHbase.oop.worker;

import cn.demo.kfkToHbase.oop.writer.IWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Worker implements  IWorker{
    private KafkaConsumer<String,String> consumer;
    private IWriter writer;

    public Worker(String topicName,String groupId,IWriter writer) {
        this.writer = writer;
        //消费者配置
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"single03:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Collections.singleton(topicName));
    }

    @Override
    public void fillData(String targetName) {
        int num = 0;
        while (true){
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(100L));
            int rstNum = writer.write(targetName, poll);
            num+=rstNum;
            System.out.println("---------------------num: "+num);
           /* try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/
        }
    }
}
