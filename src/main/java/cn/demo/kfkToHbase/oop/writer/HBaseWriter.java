package cn.demo.kfkToHbase.oop.writer;

import cn.demo.kfkToHbase.oop.handler.IParseRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.util.List;

public class HBaseWriter implements IWriter{
    private Connection connection = null;
    private IParseRecord handler = null;
    private List<Put> datas= null;
    private Table table = null;
    private BufferedMutator mutator = null;

    public HBaseWriter(String hbase_dir, String zk_quorum, String client_port,IParseRecord handler) {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.HBASE_DIR,hbase_dir);
        conf.set(HConstants.ZOOKEEPER_QUORUM,zk_quorum);
        conf.set(HConstants.CLIENT_PORT_STR,client_port);
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.handler = handler;
    }

    public HBaseWriter(IParseRecord handler) {
        this("hdfs://192.168.42.122:9000/hbase",
                "192.168.42.122",
                "2181",handler);
    }

    public HBaseWriter(String hbase_dir, String zk_quorum,IParseRecord handler) {
        this(hbase_dir,zk_quorum,"2181",handler);
    }


    @Override
    public int write(String targetTable, ConsumerRecords<String, String> records) {
        if (mutator==null){
            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(targetTable));
            params.writeBufferSize(10*1024*1024L);   //缓存 大小为10M
            params.setWriteBufferPeriodicFlushTimeoutMs(5*1000L); //刷新 大小5s
            try {
                mutator = connection.getBufferedMutator(params);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        datas = handler.parse(records);
        try {
            mutator.mutate(datas);
            mutator.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return datas.size();

       /* try {
            if (table==null)
            table = connection.getTable(TableName.valueOf(targetTable));
            if (datas!=null)
                datas.clear();
            datas = handler.parse(records);
            if (datas!=null && datas.size()!=0){
                table.put(datas);
                table.close();
                return datas.size();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;*/
    }
}
