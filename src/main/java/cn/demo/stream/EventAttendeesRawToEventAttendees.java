package cn.demo.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EventAttendeesRawToEventAttendees {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG,"earToeaKafkaStream");
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"single03:9092");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        //创建流构造器
        StreamsBuilder builder = new StreamsBuilder();

        //构建好builder，将event_attendees_raw中的数据经过flatMap处理写入到event_attendees2中
        KStream<Object, Object> ear = builder.stream("event_attendees_raw");

        ear.flatMap((key,value)->{
            ArrayList<KeyValue<String,String>> list = new ArrayList<>();
            String[] fields = value.toString().split(",");
            for (int i = 2; i <= 5 ; i++) {
                if (fields.length>=i && fields[i-1].trim().length()>0) {
                    String[] users = fields[i-1].trim().split(" ");
                    String status = "yes";
                    if (i==3)
                        status = "maybe";
                    else if(i==4)
                        status = "invited";
                    else if(i==5)
                        status = "no";
                    for(String user : users){
                        String info = fields[0]+","+user+","+status;
                        System.out.println(info);
                        KeyValue<String,String> keyValue = new KeyValue<>(null,info);
                        list.add(keyValue);
                    }
                }
            }
            return list;
        }).to("event_attendees2");

        Topology topo = builder.build();
        KafkaStreams streams = new KafkaStreams(topo, prop);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("stream"){
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
