package cn.demo.kfkToHbase.oop.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class EventsHandler implements IParseRecord{
    @Override
    public List<Put> parse(ConsumerRecords<String, String> records) {
        List<Put> datas = new ArrayList<>();

        for (ConsumerRecord record : records){
            System.out.println(record.value().toString());
            String[] events = record.value().toString().split(",",-1);

            Put put = new Put(events[0].getBytes()); //rowKey
            put.addColumn("creator".getBytes(),"user_id".getBytes(),events[1].getBytes());
            put.addColumn("schedule".getBytes(),"start_time".getBytes(),events[2].getBytes());
            put.addColumn("location".getBytes(),"city".getBytes(),events[3].getBytes());
            put.addColumn("location".getBytes(),"state".getBytes(),events[4].getBytes());
            put.addColumn("location".getBytes(),"zip".getBytes(),events[5].getBytes());
            put.addColumn("location".getBytes(),"country".getBytes(),events[6].getBytes());
            put.addColumn("location".getBytes(),"lat".getBytes(),events[7].getBytes());
            put.addColumn("location".getBytes(),"lng".getBytes(),events[8].getBytes());
            put.addColumn("remark".getBytes(),"category".getBytes(),events[9].getBytes());
            datas.add(put);
        }
        return datas;
    }
}
