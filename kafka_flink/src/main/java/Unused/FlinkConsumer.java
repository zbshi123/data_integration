package Unused;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.Properties;

public class FlinkConsumer {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("group.id","test_flink");
        DataStream<String> input = env.readTextFile("/home/shizengbing/softwares/special.txt");
        //DataStream<String> input = env.addSource(new FlinkKafkaConsumer<String>
        //        ("test",new SimpleStringSchema(),props));
        input.print();
        env.execute("Flink Streaming Java API Skeleton");
    }
}
