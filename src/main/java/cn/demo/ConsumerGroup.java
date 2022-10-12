package cn.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class ConsumerGroup {
    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    Properties prop = new Properties();
                    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.42.122:9092");
                    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

                    prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
                    prop.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");

                    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

                    prop.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
                    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
                    consumer.subscribe(Collections.singleton("sensor2"));
                    // 获取消费者所分配到的分区
                    Set<TopicPartition> assignment = consumer.assignment();
                    System.out.println(assignment);
                    for (TopicPartition tp : assignment) {
                        // 参数1： 表示分区。参数2： 表示制定从分区的哪个位置开始消费
                        consumer.seek(tp, 1);
                    }


                    while (true) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
                        for (ConsumerRecord record : records) {
                            System.out.println(
                                      record.topic() + " "
                                    + record.partition() + " "
                                    + record.offset() + " "
                                    + record.value() + " "
                                    + Thread.currentThread().getName()
                            );
                        }
                    }
                }
            });
            thread.start();
        }
    }
}
