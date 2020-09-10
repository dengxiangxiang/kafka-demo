package kafka.producer;

import kafka.producer.interceptor.CounterInterceptor;
import kafka.producer.interceptor.TimeInterceptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CustomProducer {

    public static void main(String[] args) throws Exception {
        // 1 设置配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        props.put("retries", 0);
        props.put("delivery.timeout.ms",120000); // default 2min

        props.put("batch.size", 16384); // 16Kb
        props.put("linger.ms", 0); // waiting for batch before sending out

        props.put("buffer.memory", 33554432); //total bytes of memory the producer can use to buffer records waiting to be sent to the server
        props.put("max.block.ms",60000); //1min, how long the methods send() or partitionsFor() will block because the buffer is full or metadata is unavailable

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        props.put("partitioner.class",org.apache.kafka.clients.producer.internals.DefaultPartitioner.class);

        //Note that enabling idempotence requires
        // max.in.flight.requests.per.connection to be less than or equal to 5,
        // retries to be greater than 0
        // and acks must be 'all'.
        props.put("enable.idempotence",false);


        // 2 构建拦截链
        List<String> interceptors = new ArrayList<>();
        interceptors.add(TimeInterceptor.class.getName());
        interceptors.add(CounterInterceptor.class.getName());
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        String topic = "test";
        Producer<String, String> producer = new KafkaProducer<>(props);

        // 3 发送消息

        for (int i=0;i<10000;i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "message" + i);
            producer.send(record);
            if(i==10){
                throw new Exception("Runtime Exception!");
            }
            Thread.sleep(1000);
        }

        // 4 一定要关闭producer，这样才会调用interceptor的close方法
        producer.close();
    }
}

