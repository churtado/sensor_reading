package sensor.functions;

import com.github.churtado.sensor.avro.SensorReading;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;

public class AverageFunction implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<SensorReading> collector) throws Exception {

        Double sum = 0.0;
        Double count = 0.0;

        for(SensorReading r: iterable) {
            sum += r.getReadingValue();
            count ++;
        }

        Double avg = sum / count;
        SensorReading result = new SensorReading();
        result.setSensorId(s);
        result.setReadingTimestamp(new DateTime(timeWindow.getEnd()));
        result.setReadingValue(avg);
        collector.collect(result);

    }
}
