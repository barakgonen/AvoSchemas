package org.producer.bg;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.bg.avro.structures.base.objects.Coordinate;
import org.bg.avro.structures.base.objects.Employee;
import org.bg.avro.structures.base.objects.NullableTime;
import org.bg.avro.structures.base.objects.Time;
import org.bg.avro.structures.objects.Manager;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws IOException {
        System.out.println("BGBG");
        Manager manager = Manager.newBuilder()
                .setEmployeeProp(Employee.newBuilder()
                        .setActive(true)
                        .setSalary(12323)
                        .setName("bgbg")
                        .build())
                .setHappy(true)
                .setNullableTime(NullableTime.newBuilder().setTimeMillis(DateTime.now().toDateTime().getMillis()).build())
                .setPosition(Coordinate.newBuilder().setAltitude(23).setLat(233).setLon(2331).build())
                .setMustAppearTimeField(Time.newBuilder().setTimeMllis(231111111).build())
                .build();
        System.out.println(manager);
//        manager.getSchema().getFields().stream().forEach(field -> {
//            String fieldName = StringUtils.capitalize(field.name());
//            switch (field.schema().getType()){
//                case RECORD:
//                    System.out.println("HANDLE RECORD BRO");
//                    break;
//                case ENUM:
//                    break;
//                case ARRAY:
//                    break;
//                case MAP:
//                    break;
//                case UNION:
//                    break;
//                case FIXED:
//                case STRING:
//                case BYTES:
//                case INT:
//                case LONG:
//                case FLOAT:
//                case DOUBLE:
//                case BOOLEAN:
//                    try {
//                        Method method = Class.forName(manager.getClass().getName()).getMethod("get" + fieldName);
//                        System.out.println("field name: " + fieldName + ", getValue(): " + method.invoke(manager));
//                    } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InvocationTargetException e) {
//                        e.printStackTrace();
//                    }
//                    break;
//                case NULL:
//                    break;
//            }
//        });
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
//        props.put("schema.registry.url", "http://192.168.227.132:8081");
//        Producer<String, Manager> producer = new KafkaProducer<>(props);
//        for (int i = 0; i < 100; i++)
//            producer.send(new ProducerRecord<String, Manager>("my-bg-topic-avro", Integer.toString(i), manager));
//
//        producer.close();
//        manager.getSchema().getFields().stream().forEach(field -> {System.out.println(field.name());});
//        manager.getSchema().getFields().stream().forEach(field -> System.out.println(field.getObjectProps().get("name")));
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://192.168.227.132:8081");
        KafkaProducer producer = new KafkaProducer(props);
        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient("http://192.168.227.132:8081", 10000);
        KafkaAvroSerializer serializer = new KafkaAvroSerializer(client);

//        String key = "key1";
//        String userSchema = "{\"type\":\"record\"," +
//                "\"name\":\"myrecord\"," +
//                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
//        Schema.Parser parser = new Schema.Parser();
//        Schema schema = parser.parse(userSchema);
//        GenericRecord avroRecord = new GenericData.Record(schema);
//        avroRecord.put("f1", "value1");
        try {
            client.register(manager.getSchema().getName() + "-value", manager.getSchema());
            ProducerRecord<String, byte[]> record = new ProducerRecord<>("topic6", "bgbg", serializer.serialize("Manager", manager));
            try {
                producer.send(record, (metadata, exception) -> System.out.println("sent"));
            } catch (SerializationException e) {
                // may need to do something with it
            } finally {
                producer.flush();
                producer.close();
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (RestClientException e) {
            e.printStackTrace();
        }


//        GenericRecord avroRecord = new GenericData.Record(manager.getSchema());
//        ProducerRecord<Object, Object> record = new ProducerRecord<>("my-bg-topic-avroo", "bggbgb", avroRecord);

// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
// then close the producer to free its resources.

    }
}
