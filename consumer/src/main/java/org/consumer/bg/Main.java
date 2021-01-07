package org.consumer.bg;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.clients.consumer.*;
import org.bg.avro.structures.base.objects.Kind;
import org.bg.avro.structures.objects.Manager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class Main {
    public static void main(String[] str) throws InterruptedException {
        String topicName = "BYTE_TOPIC";

        final Consumer<String, byte[]> consumer = new KafkaConsumer<>(getProps());
        consumer.subscribe(Arrays.asList(topicName));
        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(100);
                for (ConsumerRecord<String, byte[]> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                    Manager n = Manager.fromByteBuffer(ByteBuffer.wrap(record.value()));
                    System.out.println(n);
//                    System.out.println(record.value().getWorkerKind());
//                    if (record.value().getWorkerKind() == Kind.TRIANGLE){
//                        System.out.println("BGBGBG");
//                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
    private static Properties getProps() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "grsoup2s2ssd1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", "http://192.168.227.132:8081");

        return props;
    }
}
