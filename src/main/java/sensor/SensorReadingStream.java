package sensor;

import com.github.churtado.sensor.avro.SensorReading;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SensorReadingStream {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = setupFlink();
        InfluxDBConfig influxDBConfig = setupInfluxDBConfig();
        FlinkKafkaConsumer011<SensorReading> consumer011 = setupKafkaConsumer();

        // create a stream and consume from kafka using schema registry
        DataStreamSource<SensorReading> readings = env.addSource(consumer011);

        SingleOutputStreamOperator<String> mapToString = readings
                .map((MapFunction<SensorReading, String>) SpecificRecordBase::toString);

        mapToString.print();

        env.execute();

    }

    private static StreamExecutionEnvironment setupFlink() {

        // set up flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Important to enable checkpointing in order to recover from failure
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(4);

        return env;

    }

    private static InfluxDBConfig setupInfluxDBConfig() {

        // setup influxdb
        InfluxDBConfig influxDBConfig = InfluxDBConfig.builder("http://localhost:8086", "admin", "password", "sensor_readings")
                .batchActions(1000)
                .flushDuration(100, TimeUnit.MILLISECONDS)
                .enableGzip(true)
                .build();

        return influxDBConfig;

    }

    private static FlinkKafkaConsumer011<SensorReading> setupKafkaConsumer() {

        // setup kafka consumer
        String bootstrapServer = "localhost:29092";
        String groupId = "SensorReadingGroup";
        String zookeeper = "localhost:2181";

        Properties config = new Properties();
        config.setProperty("bootstrap.servers", bootstrapServer);
        config.setProperty("group.id", groupId);
        config.setProperty("zookeeper.connect", zookeeper);
        config.setProperty("specific.avro.reader", "true");

        String schemaRegistryUrl = "http://localhost:8081";
        String topic = "postgres_event_avro_sensor_reading";

        FlinkKafkaConsumer011<SensorReading> consumer011 = new FlinkKafkaConsumer011<SensorReading>(
                topic,
                ConfluentRegistryAvroDeserializationSchema.forSpecific(SensorReading.class, schemaRegistryUrl),
                config);
        consumer011.setStartFromEarliest();

        return consumer011;
    }

}
