package sensor.config;

import com.github.churtado.sensor.avro.SensorReading;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SensorReading> {

    public SensorTimeAssigner(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(SensorReading reading) {
        return reading.getReadingTimestamp().getMillis();
    }

}
