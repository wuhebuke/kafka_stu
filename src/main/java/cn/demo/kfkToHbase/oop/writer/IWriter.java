package cn.demo.kfkToHbase.oop.writer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/*
* 写操作
* 通过IWrite完成将数据写入的目标数据库的功能
* */
public interface IWriter {
    /*
    * 此方法完成写入操作
    * @param targetTable 目标数据库表名
    * @param records     从kafka中提取的记录信息
    * @return            影响的行数
    * */
    int write(String targetTable, ConsumerRecords<String,String> records);
}
