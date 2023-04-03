import PO.RawData;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.Properties;

import java.util.HashMap;
import java.util.Map;

public class Client {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map<String, String> globalParameters = new HashMap<>();
        // ClickHouse cluster properties
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, MyClickHouseLink.HOST+":8123");
        // sink common
        globalParameters.put(ClickHouseSinkConst.TIMEOUT_SEC, "1");
        //globalParameters.put(ClickHouseSinkConst.FAILED_RECORDS_PATH, "/home/temp");
        globalParameters.put(ClickHouseSinkConst.NUM_WRITERS, "2");
        globalParameters.put(ClickHouseSinkConst.NUM_RETRIES, "2");
        globalParameters.put(ClickHouseSinkConst.QUEUE_MAX_CAPACITY, "10");
        globalParameters.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, "false");

        // set global paramaters
        ParameterTool parameters = ParameterTool.fromMap(globalParameters);
        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(1);

        Properties properties = new Properties();
        //这里是由一个kafka 连接属性
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "201250197");

        //第一个参数是topic的名称
        //DataStream<String> inputStream=env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));
        // source
        //DataStream<String> inputStream = env.readTextFile("/mnt/SharedFile/test.txt");
        DataStream<String> inputStream = env.readTextFile("data1.txt");
        // DataStream<String> inputStream = env.readTextFile("D:\\DATA\\DataIntegrate\\record.txt","utf-8");
        //inputStream.print();
        // Transform 操作
        SingleOutputStreamOperator<RawData> dataStream = inputStream.map((MapFunction<String, RawData>) input ->{
            RawData rawData =null;
            try {
                rawData = JSON.parseObject(input, RawData.class);
                return rawData;
            }catch (Exception e){
                e.printStackTrace();
                System.out.println("Error:"+input);
                System.out.println("eventBody:"+ rawData.getEventBody());
            }
            return null;
        });

        // dataStream.addSink(new ClickHouseSinkFunction());
        dataStream.addSink(new MyClickHouseLink());
        //dataStream.print();

        env.execute("clickhouse sink multi-thread");
    }
}
