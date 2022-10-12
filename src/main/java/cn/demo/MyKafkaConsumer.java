package cn.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MyKafkaConsumer {
    public static void main(String[] args){
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.42.122:9092");
        //或prop.put("bootstrap.servers","single03:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);

        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        //earliest latest
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,"kafkaConsumerGroup1");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(prop);
        kafkaConsumer.subscribe(Collections.singleton("sensor"));
//        kafkaConsumer.poll(100);
//        Set<TopicPartition> assignment = kafkaConsumer.assignment();
//        for (TopicPartition tp:assignment){
//            if (tp.partition()==0)
//                kafkaConsumer.seek(tp,3);
//        }


        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100L));
            for (ConsumerRecord<String,String> record:records) {
                System.out.println(record.offset()+" "+record.key()+" "+ record.value());
            }
            kafkaConsumer.commitAsync(); //手动提交
        }
    }
}
