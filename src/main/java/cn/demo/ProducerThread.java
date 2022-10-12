package cn.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerThread {
    public static void main(String[] args) {

        for (int i = 0; i < 100; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    Properties prop = new Properties();
                    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.42.122:9092");
                    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
                    prop.put(ProducerConfig.ACKS_CONFIG,"1");
                    KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
                    for (int j = 0; j < 100000; j++) {
                        String context = Thread.currentThread().getName() + " " + j;
                        ProducerRecord<String, String> sensor2 = new ProducerRecord<>("sensor2", context);
                        System.out.println(context);
                        producer.send(sensor2);
                        if (j%1000==0){
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }).start();
        }
    }
}
