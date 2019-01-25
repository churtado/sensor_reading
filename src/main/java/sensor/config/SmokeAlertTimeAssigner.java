package sensor.config;

import com.github.churtado.sensor.avro.SensorReading;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import sensor.utils.SmokeAlert;

public class SmokeAlertTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SmokeAlert> {

    public SmokeAlertTimeAssigner(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(SmokeAlert smokeAlert) {
        return smokeAlert.timestamp;
    }

}
