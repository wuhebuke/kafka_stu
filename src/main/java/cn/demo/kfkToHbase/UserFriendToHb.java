package cn.demo.kfkToHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

/*
* 非oop方法
* */
public class UserFriendToHb {
    public static void main(String[] args) {
        //消费者配置
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"single03:9092");
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        prop.put(ConsumerConfig.GROUP_ID_CONFIG,"user_friend_group1");
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        consumer.subscribe(Collections.singleton("user_friends"));

        //配置hbase信息，连接hbase数据库
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.HBASE_DIR,"hdfs://192.168.42.122:9000/hbase");
        conf.set(HConstants.ZOOKEEPER_QUORUM,"192.168.42.122");
        conf.set(HConstants.CLIENT_PORT_STR,"2181");

        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table userFriendTable = connection.getTable(TableName.valueOf("events_db:user_friend"));

            int num = 0;
        while (true){
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(100L));
            ArrayList<Put> datas = new ArrayList<>();
            for (ConsumerRecord<String, String> record : poll){
                //组装 从kafka取出的数据转换成hbase的put对象，装入集合datas中
                System.out.println(record.value().toString());
                String[] split = record.value().split(",");

                Put put = new Put(Bytes.toBytes((split[0] + split[1]).hashCode())); //rowKey
                put.addColumn("uf".getBytes(),"userid".getBytes(),split[0].getBytes());
                put.addColumn("uf".getBytes(),"friendid".getBytes(),split[1].getBytes());
                datas.add(put);
            }

            if (datas.size()!=0){
                //统计从kafka中取出的数量，并将put集合写入的hbase中
                num+=datas.size();
                System.out.println("--------------------------------------num: "+num);
                userFriendTable.put(datas);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
          }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
