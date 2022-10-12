package cn.demo.kfkToHbase.oop.handler;

import org.apache.hadoop.hbase.client.Put;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.List;

/*
* 传入ConsumerRecords对象，返回ArrayList<Put>对象
* */
public interface IParseRecord {
    List<Put> parse(ConsumerRecords<String,String> records);
}
