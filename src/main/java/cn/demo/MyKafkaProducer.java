package cn.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class MyKafkaProducer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.42.122:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        //0 1 -1
        prop.put(ProducerConfig.ACKS_CONFIG,"0");

        String tag = "1";
        Scanner scanner = new Scanner(System.in);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);
        while (tag.equals("1")){
            System.out.print("请输入内容：");
            String text = scanner.nextLine();
            ProducerRecord<String, String> record = new ProducerRecord<>("sensor", text);
            kafkaProducer.send(record);
            System.out.print("是否退出(0退出，1继续) ");
            tag = scanner.nextLine();
        }
    }
}
