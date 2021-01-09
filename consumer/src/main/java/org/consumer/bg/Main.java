package org.consumer.bg;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import org.bg.avro.structures.base.objects.CoordinateEnumJson;
import org.bg.avro.structures.base.objects.Suit;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Main {


    // Copied from example of confluent
    private static void genericAvroListener() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "grouap1sd");


        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
//        props.put("schema.registry.url", "http://192.168.227.132:8081");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String topic = "MY_TEST_TOPIC";
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
            for (ConsumerRecord<byte[], byte[]> record : records) {
                System.out.printf("Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), deserialize(record.key()), deserialize(record.value()));

                CoordinateEnumJson d = deserialize(record.value());
                System.out.println(d);
            }

            consumer.commitSync();
        }
    }

    private static <V> V deserialize(final byte[] objectData) {
        return (V) org.apache.commons.lang3.SerializationUtils.deserialize(objectData);
    }

    private static void specificAvroListener() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1ssssasdsdd");


        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        props.put("schema.registry.url", "http://192.168.227.132:8081");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String topic = "TRIANGLEEE";
        final Consumer<String, CoordinateEnumJson> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));


        try {
            while (true) {
                ConsumerRecords<String, CoordinateEnumJson> records = consumer.poll(100);
                for (ConsumerRecord<String, CoordinateEnumJson> record : records) {
                    CoordinateEnumJson s = record.value();
                    if (s.getEnum$().ordinal() == Suit.TRIANGLE.ordinal()) {
                        System.out.println("key: " + record.key() + " , BGBG got TRIANGLE");
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] str) throws InterruptedException {
//        genericAvroListener();
        specificAvroListener();
    }
}