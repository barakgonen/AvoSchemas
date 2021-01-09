package org.producer.bg;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.bg.avro.structures.base.objects.Coordinate;
import org.bg.avro.structures.base.objects.CoordinateEnumJson;
import org.bg.avro.structures.base.objects.Suit;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class Main {


    public static void main(String[] args) throws IOException {
//        Manager manager = getManager();
        produceUsingAvroSchemaSerializer();
//        produceGenericByteArray();
    }

    private static void produceGenericByteArray() {

        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "localhost:9092");
        producerConfig.put("client.id", "basic-producer");
        producerConfig.put("acks", "all");
        producerConfig.put("retries", "3");
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        SimpleProducer producer = new SimpleProducer(producerConfig, false);
        String topic = "MY_TEST_TOPIC";

        CoordinateEnumJson f = CoordinateEnumJson.newBuilder()
                .setPos(
                        Coordinate.newBuilder().setAltitude(23).setLat(234).setLon(231).build()
                ).setEnum$(Suit.TRIANGLEYYYY).build();
        for (int i = 0; i < 5; i++) {
            producer.send(topic, serialize("BGBG"), serialize(f));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }

    public static byte[] serialize(final Object obj) {
        return org.apache.commons.lang3.SerializationUtils.serialize((Serializable) obj);
    }

    private static void produceUsingAvroSchemaSerializer() {
        // Copy pasted example from official confluent at https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html
        // Just changing the record to Manager which is the type i generated from Avro schema
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REFLECTION_CONFIG, "true");
        props.put("schema.registry.url", "http://192.168.227.132:8081");
        KafkaProducer producer = new KafkaProducer(props);

        try {
            for (int i = 0; i < 1000; i++) {
                CoordinateEnumJson f = CoordinateEnumJson.newBuilder()
                        .setPos(
                                Coordinate.newBuilder().setAltitude(Double.valueOf(i * 0.01)).setLat(Double.MAX_VALUE).setLon(Double.MIN_VALUE).build()
                        ).setEnum$(Suit.TRIANGLEYYYY).build();
                ProducerRecord<String, CoordinateEnumJson> record = new ProducerRecord<>("TRIANGLEEE", "key" + i, f);
                producer.send(record);
            }
        } catch (SerializationException e) {
            // may need to do something with it
        }
        // When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
        // then close the producer to free its resources.
        finally {
            producer.flush();
            producer.close();
        }

    }

//        private void <D extends GenericRecord>  produceGenericRecord(String topic, )
}
//        });

//        obj.getSchema().getFields().stream().forEach(field -> genericRecord.put(field.name(), field.toString()));
//                System.out.println("bgbg");
//
//                // Building coordinate back from generic record
//                Schema writerSchema = genericRecord.getSchema();
//
//                Coordinate c = new Coordinate();
//                writerSchema.getFields().stream().forEach(field -> {
//                String fieldName = StringUtils.capitalize(field.name());
//                switch (field.schema().getType()) {
//                case RECORD:
//                System.out.println("HANDLE RECORD BRO");
//                break;
//                case ENUM:
//                break;
//                case ARRAY:
//                break;
//                case MAP:
//                break;
//                case UNION:
//                break;
//                case FIXED:
//                case STRING:
//                case BYTES:
//                case INT:
//                case LONG:
//                case FLOAT:
//                case DOUBLE:
//                case BOOLEAN:
//                try {
//                Object fieldValue = genericRecord.get(field.name());
//                Method method = Class.forName(obj.getClass().getName()).getMethod("set" + fieldName);
//                System.out.println("field name: " + fieldName + ", getValue(): " + method.invoke(c, fieldValue));
//                } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InvocationTargetException e) {
//                e.printStackTrace();
//                }
//                break;
//                case NULL:
//                break;
//                }
//                });
//
//                if (c.equals(obj))
//                System.out.println("BGBG");