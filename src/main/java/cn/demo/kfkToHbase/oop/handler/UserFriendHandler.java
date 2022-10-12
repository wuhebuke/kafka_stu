package cn.demo.kfkToHbase.oop.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class UserFriendHandler implements IParseRecord{
    @Override
    public List<Put> parse(ConsumerRecords<String, String> records) {
       List<Put> datas = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records){
            //组装 从kafka取出的数据转换成hbase的put对象，装入集合datas中
            System.out.println(record.value().toString());
            String[] split = record.value().split(",");

            Put put = new Put(Bytes.toBytes((split[0] + split[1]).hashCode())); //rowKey
            put.addColumn("uf".getBytes(),"userid".getBytes(),split[0].getBytes());
            put.addColumn("uf".getBytes(),"friendid".getBytes(),split[1].getBytes());
            datas.add(put);
        }
        return datas;
    }
}
