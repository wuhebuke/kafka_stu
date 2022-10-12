package cn.demo.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class UserFriendsRawToUserFriends {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG,"ufrToufKafkaStream");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"single03:9092");
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> stream = builder.stream("user_friends_raw");

        stream.flatMap((key,value)->{
            ArrayList<KeyValue<String,String>> list = new ArrayList<>();
            String[] user_friends = value.toString().split(",");
            if (user_friends.length==2) {
                String userId = user_friends[0];
                String[] friends = user_friends[1].split(" ");
                for(String friend : friends){
                    String info = userId+","+friend;
                    System.out.println(info);
                    KeyValue<String,String> keyValue = new KeyValue<>(null,info);
                    list.add(keyValue);
                }
            }

            return list;
        }).to("user_friends2");

        Topology topo = builder.build();
        KafkaStreams streams = new KafkaStreams(topo, prop);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("stream")
        {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
