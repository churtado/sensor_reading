package sensor;

import com.github.churtado.sensor.avro.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sensor.config.SensorTimeAssigner;
import sensor.functions.AverageFunction;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SensorReadingStream {

    static Logger logger = LoggerFactory.getLogger(SensorReadingStream.class.getName());

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = setupFlink();
        InfluxDBConfig influxDBConfig = setupInfluxDBConfig();
        FlinkKafkaConsumer011<SensorReading> consumer011 = setupKafkaConsumer();

        // create a stream and consume from kafka using schema registry
        DataStreamSource<SensorReading> readings = env.addSource(consumer011);

        // raw numbers
        readings
                .setParallelism(4)
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5)))
                .map( new MapSensorReadingToInfluxDb("temperature_readings") )
                .addSink(new InfluxDBSink(influxDBConfig));


        // window by 5 seconds, calculate avg and sink to influxdb
//        readings
//                .setParallelism(4)
//                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5)))
//                .keyBy(new KeySelector<SensorReading, String>() {
//                    @Override
//                    public String getKey(SensorReading sensorReading) throws Exception {
//                        return sensorReading.getSensorId();
//                    }
//                })
//                .timeWindow(Time.seconds(5))
//                .apply(new AverageFunction())
//                .map(new MapToString())
//                .print();
//                .map( new MapSensorReadingToInfluxDb("5s_temp_avg") )
//                .addSink(new InfluxDBSink(influxDBConfig));

        env.execute();

    }

    private static StreamExecutionEnvironment setupFlink() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(4);

        return env;

    }

    private static InfluxDBConfig setupInfluxDBConfig() {

        String url = "http://localhost:8086";
        String username = "admin";
        String password = "password";
        String database = "sensor_readings";

        // setup influxdb
        InfluxDBConfig influxDBConfig = InfluxDBConfig.builder(url, username, password, database)
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

class MapSensorReadingToInfluxDb extends RichMapFunction<SensorReading, InfluxDBPoint>{

    String measurement;
    Iterable<String> tags;
    Iterable<String> fields;

    public MapSensorReadingToInfluxDb(String measurement) {
        this.measurement = measurement;
        this.tags = tags;
        this.fields = fields;
    }

    @Override
    public InfluxDBPoint map(SensorReading s) throws Exception {

        HashMap<String, String> tags = new HashMap<>();
        tags.put("sensor", s.getSensorId());

        HashMap<String, Object> fields = new HashMap<>();
        fields.put("temperature", s.getReadingValue());

        return new InfluxDBPoint(this.measurement, s.getReadingTimestamp().getMillis(), tags, fields);
    }
}

class MapToString implements MapFunction<SensorReading, String> {

    @Override
    public String map(SensorReading sensorReading) throws Exception {
        return sensorReading.toString();
    }
}