// kafka生产者代码
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
public class MyProducer {


    public static void main(String[] args) {

        Properties prop=new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        prop.put(ProducerConfig.ACKS_CONFIG,"-1");
        //创建生产者
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);

        String fileName = "/mnt/SharedFile/test.txt";
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("Read Start：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                //System.out.println("line " + line + ": " + tempString);
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("test", tempString);
                if (line % 1000 == 0) {
                    System.out.print("line " + line + ": ");
                    System.out.println(producerRecord);
                }

                producer.send(producerRecord);

                try {
                    Thread.sleep(12);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (line > 10000000) break;
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        System.out.println("Produce Done");
    }
}
