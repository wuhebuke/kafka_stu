package cn.demo.kfkToHbase.oop.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class UsersHandler implements IParseRecord{
    @Override
    public List<Put> parse(ConsumerRecords<String, String> records) {
        List<Put> datas = new ArrayList<>();
        for (ConsumerRecord record : records){
            System.out.println(record.value().toString());
            String[] users = record.value().toString().split(",",-1);

            Put put = new Put(users[0].getBytes());
            put.addColumn("profile".getBytes(),"birth_year".getBytes(),users[2].getBytes());
            put.addColumn("profile".getBytes(),"gender".getBytes(),users[3].getBytes());
            put.addColumn("region".getBytes(),"locale".getBytes(),users[1].getBytes());
            put.addColumn("region".getBytes(),"location".getBytes(),users[5].getBytes());
            put.addColumn("region".getBytes(),"time_zone".getBytes(),users[6].getBytes());
            put.addColumn("registration".getBytes(),"joined_at".getBytes(),users[4].getBytes());
            datas.add(put);
        }
        return datas;
    }
}
