package org.producer.bg;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.StringUtils;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

public class Main {

    private static Properties getProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://192.168.227.132:8081");
        return props;
    }

    private static Manager getManager() {
        return Manager.newBuilder()
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
    }

    private static <T extends SpecificRecordBase> void produceGenericRecord(String topic, T obj) {
        KafkaProducer producer = new KafkaProducer(getProducerProps());
        String key = obj.getSchema().getName();
        ProducerRecord<Object, T> record = new ProducerRecord<>(topic, key, obj);
        try {
            producer.send(record);
        } catch(SerializationException e) {
            // may need to do something with it
        }
// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
// then close the producer to free its resources.
        finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String[] args) throws IOException {
        Manager manager = getManager();
//        produceGenericRecord("BYTE_TOPIC", manager);

        KafkaProducer producer = new KafkaProducer(getProducerProps());
        String key = manager.getSchema().getName();
        ProducerRecord<Object, byte[]> record = new ProducerRecord<>("BYTE_TOPIC", key, manager.toByteBuffer().array());
        try {
            producer.send(record);
        } catch(SerializationException e) {
            // may need to do something with it
        }
// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
// then close the producer to free its resources.
        finally {
            producer.flush();
            producer.close();
        }
    }

//        GenericRecord avroRecord = new GenericData.Record(manager.getSchema());
//        ProducerRecord<Object, Object> record = new ProducerRecord<>("my-bg-topic-avroo", "bggbgb", avroRecord);

// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
// then close the producer to free its resources.


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