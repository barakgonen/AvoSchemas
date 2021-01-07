package org.producer.bg;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class KafkaAvroSerializer<T extends GenericContainer> implements Serializer<T> {

    private SchemaProvider schemaProvider;
    private CachedSchemaRegistryClient client;

    public KafkaAvroSerializer(String schemaRegistryUrl){
        super();
        client = new CachedSchemaRegistryClient(schemaRegistryUrl, 10000);
    }
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
//        schemaProvider = AvroSchemaUtils.getSchemaProvider(configs);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            int schemaID = client.register(topic, data.getSchema());
            writeSchemaId(stream, schemaID);
            writeSerializedAvro(stream, data, data.getSchema());
            return stream.toByteArray();
        } catch (IOException | RestClientException e) {
            throw new RuntimeException("Could not serialize data", e);
        }
    }

    private void writeSchemaId(ByteArrayOutputStream stream, int id) throws IOException {
        try (DataOutputStream os = new DataOutputStream(stream)) {
            os.writeInt(id);
        }
    }
    private void writeSerializedAvro(ByteArrayOutputStream stream, T data, Schema schema) throws IOException {
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        DatumWriter<T> datumWriter = new GenericDatumWriter<>(schema);
        datumWriter.write(data, encoder);
        encoder.flush();
    }
}
