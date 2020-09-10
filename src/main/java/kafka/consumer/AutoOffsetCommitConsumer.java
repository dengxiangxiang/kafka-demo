package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;

public class AutoOffsetCommitConsumer {
    public static void main(String[] args) {
        new AutoOffsetCommitConsumer().start();
    }

    private Logger logger = LoggerFactory.getLogger(AutoOffsetCommitConsumer.class);

    private KafkaConsumer<String, String> consumer;

    private boolean started;
    private volatile boolean paused;
    private Thread t;

    private void buildConsumer() {

        // basic parameters
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "my-consumer-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        // data amount
        props.put("fetch.min.bytes",0);
        props.put("fetch.max.wat.ms",5000);
        props.put("max.partition.fetch.bytes",1024 * 1024); // 1M = 1024 * 1024
        props.put("max.poll.records",2);

        // heart beat
        props.put("session.timeout.ms",8000);  //3000 is not in range support by broker
        props.put("heartbeat.interval.ms",2000);

        // offset
        props.put("enable.auto.commit",true);
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset","latest"); //default: latest ,  or  earliest

        String topic = "test";

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topic));
        logger.info("kafka.topic: " + topic);
    }

    public AutoOffsetCommitConsumer() {
        t = new Thread(new Runnable() {
            @Override
            public void run() {
                startConsume();
            }
        });
    }

    private void startConsume() {
        if (consumer == null) {
            buildConsumer();
            started = true;
            paused = false;
        }
        try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                    logger.info("poll returned!");
                    List<String> values = new ArrayList<String>();
                    for (ConsumerRecord<String, String> record : records) {
                        values.add(record.value());
                        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    }

                    // deal with consumer
                    logger.info(values.toString());

                }
        } catch (WakeupException e) {
            // 正常退出的异常，直接忽略
        } finally {
            consumer.close();
            started = false;
        }
    }



    public void stop() {
        consumer.wakeup();
    }

    public void start() {
        t.start();
    }

    public void pause() {
        if (!paused) {
            logger.warn("beginning pausing kafka consumer......");
            consumer.pause(consumer.assignment());
            paused = true;
        }

    }

    public void resume() {
        if (paused) {
            logger.warn("beginning resuming kafka consumer......");
            consumer.resume(consumer.assignment());
            paused = false;
        }
    }
}
