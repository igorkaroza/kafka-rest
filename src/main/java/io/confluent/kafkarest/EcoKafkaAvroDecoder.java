package io.confluent.kafkarest;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class EcoKafkaAvroDecoder extends AbstractKafkaAvroDeserializer implements Decoder<Object>,  Deserializer<Object> {

    protected VerifiableProperties props;

    public EcoKafkaAvroDecoder(SchemaRegistryClient schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    public EcoKafkaAvroDecoder(SchemaRegistryClient schemaRegistry, VerifiableProperties props) {
        this.schemaRegistry = schemaRegistry;
        this.props = props;
        configure(deserializerConfig(props));
    }

    public EcoKafkaAvroDecoder(VerifiableProperties props) {
        this.props = props;
        configure(new KafkaAvroDeserializerConfig(props.props()));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        configure(new KafkaAvroDeserializerConfig(props.props()));
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return fromBytes(data);
    }

    @Override
    public Object fromBytes(byte[] bytes) {
        Object decoded  = null;

        try {
            decoded = deserialize(bytes);
        } catch (SerializationException e) {
            if (bytes.length == 4) { //int key
                decoded = ByteBuffer.wrap(bytes).getInt();
            } else if (bytes.length == 8) { //long key
                decoded = ByteBuffer.wrap(bytes).getLong();
            } else {
                decoded = new StringDecoder(props).fromBytes(bytes);
            }
        }

        return decoded;
    }

    @Override
    public void close() {

    }
}