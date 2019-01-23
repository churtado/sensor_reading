package sensor.serialize;


import com.github.churtado.sensor.avro.SensorReading;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

public class SensorReadingAvroSerializationSchema<T> implements SerializationSchema<T> {

    private static final long serialVersionUID = 1L;
    private final String topic;
    private transient KafkaAvroSerializer avroSerializer;
    private final String baseUrl;
    private final int identityMapCapacity;
    private boolean first_call = true;

    public SensorReadingAvroSerializationSchema(String topic, String baseUrl, int identityMapCapacity) {
        this.topic = topic;
        this.baseUrl = baseUrl;
        this.identityMapCapacity = identityMapCapacity;
    }

    @Override
    public byte[] serialize(T obj) {
        /**
         * The avro serializer is not serializable. This means it can't be
         * sent to all the copies of the operator. Because of that, we're
         * delaying the initialization of the serializer until the first invocation
         * of the serialize method. That way it's initialized lazily by the time
         * the schema has been sent to the operator task.
         */
        if(first_call) {
            Properties registryProperties = new Properties();
            registryProperties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, baseUrl);
            SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(baseUrl, identityMapCapacity);
            try {
                schemaRegistryClient.register(topic, SensorReading.getClassSchema());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RestClientException e) {
                e.printStackTrace();
            }

            this.avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
            this.avroSerializer.configure(new HashMap(registryProperties), false);
            first_call = false;
        }
        return avroSerializer.serialize(topic, obj);
    }
}