package org.consumer.bg;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.bg.avro.structures.objects.Manager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;

public class Main {
    public static void main(String[] str) throws InterruptedException {
//        Properties props = new Properties();
//
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1a");
//
//
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
//        props.put("schema.registry.url", "http://192.168.227.132:8081");
//
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        String topic = "topic1";
//        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
//        consumer.subscribe(Arrays.asList(topic));
//
//        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient("http://192.168.227.132:8081", 10000);
//
//        try {
//            while (true) {
//                ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
//                for (ConsumerRecord<String, GenericRecord> record : records) {
//
//                }
//            }
//        } finally {
//            consumer.close();
//        }
        System.out.println("Starting AutoOffsetAvroConsumerExample ...");
        readMessages();
    }

    private static void readMessages() throws InterruptedException {
        KafkaConsumer<String, byte[]> consumer = createConsumer();
        // Assign to specific topic and partition.
        consumer.subscribe(Collections.singleton("topic6"));
//        consumer.assign(Arrays.asList(new TopicPartition("TTTT", 0)));
        processRecords(consumer);
    }

    private static void processRecords(KafkaConsumer<String, byte[]> consumer) {
        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient("http://192.168.227.132:8081", 10000);

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            long lastOffset = 0;
            for (ConsumerRecord<String, byte[]> record : records) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(record.value());
                try {
//                    AvroDeserializer<, Manager> avroDeserializer = new KafkaAvroDeserializer(client);
                    Manager m = Manager.fromByteBuffer(byteBuffer);
                    System.out.println(m);
                } catch (IOException e) {
                    e.printStackTrace();
                }
//                GenericRecord genericRecord = AvroSupport.byteArrayToData(AvroSupport.getSchema(), record.value());
//                String firstName = AvroSupport.getValue(genericRecord, "firstName", String.class);
//                System.out.printf("\n\roffset = %d, key = %s, value = %s", record.offset(), record.key());
                System.out.printf("\n\roffset = %d, key = %s", record.offset(), record.key());
                lastOffset = record.offset();
            }
            System.out.println("lastOffset read: " + lastOffset);
            consumer.commitSync();
        }
    }

    private static KafkaConsumer<String, byte[]> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        String consumeGroup = "cg12s33231cs";
        props.put("group.id", consumeGroup);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "100");
        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "30000");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");

        props.put("schema.registry.url", "http://192.168.227.132:8081");

        return new KafkaConsumer<String, byte[]>(props);
    }
}
