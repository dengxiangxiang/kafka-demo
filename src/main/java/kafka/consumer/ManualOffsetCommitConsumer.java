package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class ManualOffsetCommitConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "my-group-1357");
        props.setProperty("enable.auto.commit", "false"); //*** important
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        String topic = "test";

        for (PartitionInfo info : consumer.partitionsFor(topic)) {
            System.out.println(info.toString());
        }

        consumer.subscribe(Arrays.asList(topic));

        ConsumerRecords recordTemp = consumer.poll(0);
        System.out.println(recordTemp.isEmpty());

        Map<TopicPartition, Long> offsets = getPartitionOffsetsFromDB();
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            consumer.seek(entry.getKey(), entry.getValue());
        }


        final int minBatchSize = 5;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {

                // specific business logic
                insertIntoDb(buffer);

//                Exception in thread "main" org.apache.kafka.clients.consumer.CommitFailedException: Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing the session timeout or by reducing the maximum size of batches returned in poll() with max.poll.records.
//                        at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.sendOffsetCommitRequest(ConsumerCoordinator.java:808)
//                at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.commitOffsetsSync(ConsumerCoordinator.java:691)
//                at org.apache.kafka.clients.consumer.KafkaConsumer.commitSync(KafkaConsumer.java:1334)
//                at org.apache.kafka.clients.consumer.KafkaConsumer.commitSync(KafkaConsumer.java:1298)
//                at kafka.consumer.ManualOffsetCommitConsumer.main(ManualOffsetCommitConsumer.java:52)
                consumer.commitSync();
                buffer.clear();
            }
        }

    }

    private static void insertIntoDb(List<ConsumerRecord<String, String>> buffer) {
        //- -
        // 1 begin transaction
        // 2 insert offsets of partitions into db
        // 3 insert business result into db
        // 4 commit transaction
    }

    private static Map<TopicPartition, Long> getPartitionOffsetsFromDB() {
        // load partition-offset from db
        Map<TopicPartition, Long> map = new HashMap<TopicPartition, Long>();
        map.put(new TopicPartition("test", 0), 0L);
        return map;
    }
}
