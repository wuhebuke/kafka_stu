package cn.demo.kfkToHbase.oop.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TrainHandler implements IParseRecord{
    @Override
    public List<Put> parse(ConsumerRecords<String, String> records) {
        List<Put> datas = new ArrayList<>();

        for (ConsumerRecord record : records){
            System.out.println(record.value().toString());
            String[] train= record.value().toString().split(",",-1);
            Random random = new Random();
            Put put = new Put(Bytes.toBytes((train[0]+train[1]+Math.random()).hashCode())); //rowKey
            put.addColumn("eu".getBytes(),"user".getBytes(),train[0].getBytes());
            put.addColumn("eu".getBytes(),"event".getBytes(),train[1].getBytes());
            put.addColumn("eu".getBytes(),"invited".getBytes(),train[2].getBytes());
            put.addColumn("eu".getBytes(),"time_stamp".getBytes(),train[3].getBytes());
            put.addColumn("eu".getBytes(),"interested".getBytes(),train[4].getBytes());
            put.addColumn("eu".getBytes(),"not_interested".getBytes(),train[5].getBytes());
            datas.add(put);
        }
        return datas;
    }
}
