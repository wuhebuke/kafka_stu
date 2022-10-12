package cn.demo.kfkToHbase.oop.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class EventAttendeeHandler implements IParseRecord{
    @Override
    public List<Put> parse(ConsumerRecords<String, String> records) {
        List<Put> datas = new ArrayList<>();

        for (ConsumerRecord record : records){
            System.out.println(record.value().toString());
            String[] eventAttendee= record.value().toString().split(",",-1);
            Put put = new Put((eventAttendee[0]+eventAttendee[1]+eventAttendee[2]).getBytes()); //rowKey
            put.addColumn("euat".getBytes(),"event_id".getBytes(),eventAttendee[0].getBytes());
            put.addColumn("euat".getBytes(),"friend_id".getBytes(),eventAttendee[1].getBytes());
            put.addColumn("euat".getBytes(),"state".getBytes(),eventAttendee[2].getBytes());
            datas.add(put);
        }
        return datas;
    }
}
