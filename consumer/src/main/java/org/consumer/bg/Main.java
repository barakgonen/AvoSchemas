package org.consumer.bg;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.bg.avro.structures.base.objects.CoordinateWithEnum;

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
//        props.put("schema.registry.url", "http://192.168.227.134:8081");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String topic = "MY_TEST_TOPIC";
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

//        while (true) {
//            ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
//            for (ConsumerRecord<byte[], byte[]> record : records) {
//                System.out.printf("Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), deserialize(record.key()), deserialize(record.value()));
//
//                CoordinateEnumJson d = deserialize(record.value());
//                System.out.println(d);
//            }
//
//            consumer.commitSync();
//        }
    }

//    private static <V> V deserialize(final byte[] objectData) {
//        return (V) org.apache.commons.lang3.SerializationUtils.deserialize(objectData);
//    }

    private static void specificAvroListener() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");


        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        props.put("schema.registry.url", "http://192.168.227.129:8081");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String topic = "CONFLUENT";
        final Consumer<String, CoordinateWithEnum> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Arrays.asList(topic));

//        String topic = "topic1";
//        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (true) {
                ConsumerRecords<String, CoordinateWithEnum> records = consumer.poll(100);
                for (ConsumerRecord<String, CoordinateWithEnum> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
//
//
//        try {
//            while (true) {
//                ConsumerRecords<String, CoordinateWithEnum> records = consumer.poll(100);
//                for (ConsumerRecord<String, CoordinateWithEnum> record : records) {
//                    CoordinateWithEnum s = record.value();
//                    System.out.println("Received data: " + s);
//                }
//            }
//        } finally {
//            consumer.close();
//        }
//    }
//
//    private static void confluentListener(){
//        Properties props = new Properties();
//        props.put("zookeeper.connect", "localhost:2181");
//        props.put("group.id", "group1");
//        props.put("schema.registry.url", "http://localhost:8081");
//
////        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
////        topicCountMap.put(topic, new Integer(1));
//
//        VerifiableProperties vProps = new VerifiableProperties(props);
//        KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
//        KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);
//
//        final Consumer<String, Object> consumer = new KafkaConsumer<String, Object>(props, keyDecoder, valueDecoder);
//        consumer.subscribe(Arrays.asList("CONFLUENT"));
//
//
//        try {
//            while (true) {
//                ConsumerRecords<Object, Object> records = consumer.poll(100);
//                for (ConsumerRecord<Object, Object> record : records) {
//                    CoordinateEnumJson s = (CoordinateEnumJson) record.value();
//                    if (s.getEnum$().ordinal() == Suit.TRIANGLEYYYY.ordinal()) {
//                        System.out.println("key: " + record.key() + " , BGBG got TRIANGLEYYYY");
//                    }
//                }
//            }
//        } finally {
//            consumer.close();
//        }
//
//        kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(vProps));
//
//        Map<String, List<KafkaStream>> consumerMap = consumer.createMessageStreams(
//                topicCountMap, keyDecoder, valueDecoder);
//        KafkaStream stream = consumerMap.get(topic).get(0);
//        ConsumerIterator it = stream.iterator();
//        while (it.hasNext()) {
//            MessageAndMetadata messageAndMetadata = it.next();
//            try {
//                String key = (String) messageAndMetadata.key();
//                String value = (IndexedRecord) messageAndMetadata.message();
//            } catch(SerializationException e) {
//                // may need to do something with it
//            }
//        }
//    }
    }

    public static void main(String[] str) throws InterruptedException {
//        genericAvroListener();
//        specificAvroListener();
        specificAvroListener();
    }
}