package sensor;

import com.github.churtado.sensor.avro.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBConfig;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sensor.config.SensorTimeAssigner;
import sensor.config.SmokeAlertTimeAssigner;
import sensor.functions.PostgresAsyncFunction;
import sensor.utils.SmokeAlert;

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
                .assignTimestampsAndWatermarks(new SensorTimeAssigner(Time.seconds(5)))//.print();
                .map( new MapSensorReadingToInfluxDb() )
//                .print();
                .addSink(new InfluxDBSink(influxDBConfig));


        // smoke alerts
        DataStream<SmokeAlert> smokeAlerts = setupSmokeAlerts(env);
        smokeAlerts
                .assignTimestampsAndWatermarks(new SmokeAlertTimeAssigner(Time.seconds(5)))
                .map(new MapSmokeAlertToInfluxDb())
                .addSink(new InfluxDBSink(influxDBConfig));

        // sensor pressure using async lookup
        DataStream<Tuple2<String, Double>> sensorPressure = AsyncDataStream
                .orderedWait(
                        readings,
                        new PostgresAsyncFunction(),
                        5, TimeUnit.SECONDS,  // timeout requests after 5 seconds
                        100                   // at most 100 concurrent requests
                );
        sensorPressure
                .map(new MapPressureToInfluxDb())
                .addSink(new InfluxDBSink(influxDBConfig));

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
        consumer011.setStartFromLatest();

        return consumer011;
    }

    private static DataStream<SmokeAlert> setupSmokeAlerts(StreamExecutionEnvironment env) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:29092");

        properties.setProperty("group.id", "smoke_alerts_group");

        DataStream<SmokeAlert> result = env
                .addSource(new FlinkKafkaConsumer011<String>("smoke_alerts", new SimpleStringSchema(), properties))
                .map(new MapFunction<String, SmokeAlert>() {
                    @Override
                    public SmokeAlert map(String s) throws Exception {
                        SmokeAlert a = new SmokeAlert();
                        a.sensorId = s;
                        a.count = 1;
                        a.timestamp = new DateTime().getMillis();
                        return a;
                    }
                });

        return result;

    }

}

class MapSensorReadingToInfluxDb extends RichMapFunction<SensorReading, InfluxDBPoint>{

    @Override
    public InfluxDBPoint map(SensorReading s) throws Exception {

        HashMap<String, String> tags = new HashMap<>();
        tags.put("sensor", s.getSensorId());

        HashMap<String, Object> fields = new HashMap<>();
        fields.put("temperature", s.getReadingValue());

        return new InfluxDBPoint("temperature_readings", s.getReadingTimestamp().getMillis(), tags, fields);
    }
}

class MapSmokeAlertToInfluxDb extends RichMapFunction<SmokeAlert, InfluxDBPoint>{

    @Override
    public InfluxDBPoint map(SmokeAlert s) throws Exception {

        HashMap<String, String> tags = new HashMap<>();
        tags.put("sensor", s.sensorId);

        HashMap<String, Object> fields = new HashMap<>();
        fields.put("count", s.count);

        return new InfluxDBPoint("smoke_alerts", s.timestamp, tags, fields);
    }
}

class MapPressureToInfluxDb extends RichMapFunction<Tuple2<String, Double>, InfluxDBPoint>{

    @Override
    public InfluxDBPoint map(Tuple2<String, Double> t) throws Exception {

        HashMap<String, String> tags = new HashMap<>();
        tags.put("sensor", t.f0);

        HashMap<String, Object> fields = new HashMap<>();
        fields.put("pressure", t.f1);

        return new InfluxDBPoint("sensor_pressure", new DateTime().getMillis(), tags, fields);
    }
}

class MapToString implements MapFunction<SensorReading, String> {

    @Override
    public String map(SensorReading sensorReading) throws Exception {
        return sensorReading.toString();
    }
}