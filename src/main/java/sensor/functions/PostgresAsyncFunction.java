package sensor.functions;

import com.github.churtado.sensor.avro.SensorReading;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class PostgresAsyncFunction extends RichAsyncFunction<SensorReading, Tuple2<String, Double>> {

    QueryExecutor queryExecutor = null;
    private Connection connection;
    private final String select = "SELECT pressure FROM sensor_pressure WHERE sensor_id = ?";
    private PreparedStatement statement;

    @Override
    public void open(Configuration parameters) throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        // using pooled connections
        connection = DBCPDataSource.getConnection();
        statement = connection.prepareStatement(select);


        queryExecutor =  new QueryExecutor(connection, statement);
    }

    @Override
    public void close() throws SQLException {

        statement.close();
        connection.close();

    }

    @Override
    public void asyncInvoke(SensorReading input, ResultFuture<Tuple2<String, Double>> resultFuture) throws Exception {

        // get name from postgres table
        Future<Double> pressure = queryExecutor.getSensorPressure(input.getSensorId());


        // set the callback to be executed once the request by the client is complete
        // the callback simply forwards the result to the result future
        CompletableFuture.supplyAsync(new Supplier<Double>() {

            @Override
            public Double get() {
                try {
                    return pressure.get();
                } catch (InterruptedException | ExecutionException e) {
                    // Normally handled explicitly.
                    return null;
                }
            }
        }).thenAccept( (Double dbResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<String, Double>(input.getSensorId(), dbResult)));
        });

    }

    class QueryExecutor {

        private ExecutorService executor = Executors.newSingleThreadExecutor();
        private Connection connection;
        private PreparedStatement statement;

        public QueryExecutor(Connection connection, PreparedStatement statement) throws SQLException, ClassNotFoundException {
            this.connection = connection;
            this.statement = statement;
        }

        public Future<Double> getSensorPressure(String id) throws Exception {
            return executor.submit(() -> {

                Double result = 0.0;
                statement.setInt(1, Integer.parseInt(id));
                ResultSet rs = statement.executeQuery();
                while(rs.next()){
                    result = rs.getDouble("pressure");
                }

                rs.close();

                return result;
            });
        }
    }

    static class DBCPDataSource {

        private static BasicDataSource ds = new BasicDataSource();

        static {
            ds.setUrl("jdbc:postgresql://localhost:5432/events?user=postgres&password=password");
            ds.setUsername("postgres");
            ds.setPassword("password");
            ds.setMinIdle(5);
            ds.setMaxIdle(10);
            ds.setMaxOpenPreparedStatements(100);
        }

        public static Connection getConnection() throws SQLException {
            return ds.getConnection();
        }

        private DBCPDataSource(){ }
    }
}

class SensorInfo {
    public String sensorModel;
    public String managedBy;

    public SensorInfo(){

    }
}