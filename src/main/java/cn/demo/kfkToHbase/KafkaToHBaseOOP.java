package cn.demo.kfkToHbase;

import cn.demo.kfkToHbase.oop.handler.*;
import cn.demo.kfkToHbase.oop.worker.IWorker;
import cn.demo.kfkToHbase.oop.worker.Worker;
import cn.demo.kfkToHbase.oop.writer.HBaseWriter;
import cn.demo.kfkToHbase.oop.writer.IWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaToHBaseOOP {
    public static void main(String[] args) {
        /*IParseRecord parse = new UserFriendHandler();
        IWriter writer = new HBaseWriter(parse);
        IWorker worker = new Worker("user_friend","user_friend_group",writer);
        worker.fillData("events_db:user_friend");*/


       /* new Worker("users",
                "users_group",
                new HBaseWriter(new UsersHandler()))
                .fillData("events_db:users");*/

        /*new Worker("events",
                "events_group",
                new HBaseWriter(new EventsHandler()))
                .fillData("events_db:events");*/

        /*new Worker("event_attendees",
                "et_group",
                new HBaseWriter(new EventAttendeeHandler()))
                .fillData("events_db:event_attendee");*/

        new Worker("train",
                "train_group",
                new HBaseWriter(new TrainHandler()))
                .fillData("events_db:train");

    }
}
