package Unused;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.header.Header;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.29.4.17:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // GROUP_ID请使用学号，不同组应该使用不同的GROUP。
        // 原因参考：https://blog.csdn.net/daiyutage/article/details/70599433
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "201250xxx");
        // 原因参考：https://blog.csdn.net/matrix_google/article/details/88658234
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"student\" password=\"nju2023\";");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //consumer.subscribe(Collections.singletonList("transaction"));
        consumer.subscribe(Collections.singletonList("test"));

        // 会从最新数据开始消费
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                // 获取消息数据
                System.out.println(record.value());
                // 获取消息头
                Header groupIdHeader = record.headers().lastHeader("groupId");
                if (Objects.nonNull(groupIdHeader)) {
                    byte[] groupId = groupIdHeader.value();
                    // 此处yourGroupId替换成你们组的组号
                    if(Arrays.equals("yourGroupId".getBytes(), groupId)){
                        // 额外记录这条数据
                    }
                }
            }
        }
    }
}
