/** Daša Nosková - xnosko05
 *  VUT FIT 2024
 **/

package consumers;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.sedona.flink.SedonaContext;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.*;

@Slf4j
public class CollisionTracker {
    static String JOB_NAME = "Collision Tracker";
    public static final int CHECKPOINTING_INTERVAL_MS = 5000;
    static final String polygonsTable = "polygons";
    static final String inputTopic = "new_locations";
    static final String outputTopic = "collisions";

    static String[] locationColNames = {"id_device", "geom_point", "timestamp", "processing_time"};
    static String[] polygonColNames = {"id_polygon","geom_polygon", "valid", "creation"};

    public static void main(final String[] args) throws  Exception {

        /************************************ SETUP ********************************************/

        Properties config = new Properties();
        String fileName = "app.config";
        if (args.length > 0) {
            fileName = args[0]; // config file
        }
        try (FileInputStream fis = new FileInputStream(fileName)) {
            config.load(fis);
        } catch (FileNotFoundException ex) {
            System.out.println("Config file not found.");
        }

        String kafkaServer = config.getProperty("kafka_server");
        String db_conn_string = config.getProperty("postgres_connection_string");
        String db_username = config.getProperty("postgres_username");
        String db_password = config.getProperty("postgres_password");
        String managedMemorySize = config.getProperty("managed_memory_size");
        Properties databaseProps = setDatabaseProperties(db_conn_string, db_username, db_password, polygonsTable);

        // setup managed memory size
        Configuration configMemory = new Configuration();
        configMemory.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse(managedMemorySize));
        // set up flink's stream environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configMemory);
        env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir"); // save checkpoints to file

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        StreamTableEnvironment sedona = SedonaContext.create(env, tableEnv);

        String uniqueSuffix = Long.toString(System.currentTimeMillis());

        KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(kafkaServer)
        .setTopics(inputTopic)
        .setGroupId("collision-tracker")
        .setClientIdPrefix(uniqueSuffix)
        .setProperty("partition.discovery.interval.ms", "10000") // Dynamic Partition Discovery for scaling out topics
        .setProperty("register.consumer.metrics", "true")
        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

        /**************************************************************************************/

        // create stream
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<JsonNode> jsonStream = stream.map(CollisionTracker::mapToJson);
        jsonStream.print();
        // create factories
        TableFactory<DataStream<JsonNode>, JsonNode> locationsFactory = new LocationsTableFactory();
        TableFactory<Properties, Polygon> polygonsFactory = new PolygonsTableFactory();

        System.out.println("Collision tracker running...");

        try {
            // initialize tables
            Table polygonsWktTable = polygonsFactory.createTable(sedona, databaseProps, polygonColNames);
            Table locationWktTable = locationsFactory.createTable(sedona, jsonStream, locationColNames);

            // create tables with geometry columns
            Table locationsTable = locationsFactory.createGeometryTable(locationColNames, locationWktTable);
            Table polygonsTable = polygonsFactory.createGeometryTable(polygonColNames, polygonsWktTable);

            sedona.createTemporaryView("polygonTable", polygonsTable);
            sedona.createTemporaryView("locationTable", locationsTable);

            Table joined = sedona.sqlQuery("SELECT *, " +
                    "ST_Contains(" + polygonColNames[1] + "," + locationColNames[1] +") AS is_in_polygon "+
                    "FROM locationTable, polygonTable");

            // it's enough to keep only id of polygon at this point, the geometry isn't needed anymore, for locations all columns are needed
            Table dropPolygonCol = joined.dropColumns($(polygonColNames[1]));


            DataStream<Row> resultStream = sedona.toDataStream(dropPolygonCol);
            //resultStream.print();

            // group stream by device_id and produce collision events in json format
            DataStream<String> collisionsEvents = resultStream.keyBy(r -> (Integer) r.getField(1))
                                .flatMap(new PolygonMatchingFlatMap()).map(e -> e.toString());

            KafkaSink<String> sink = KafkaSink.<String>builder()
                    .setBootstrapServers(kafkaServer)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(outputTopic)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .setKeySerializationSchema(new MD5KeySerializationSchema())
                            .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setProperty("transaction.timeout.ms", "60000")
                    .setProperty("enable.idempotence", "true")
                    .setTransactionalIdPrefix("flink-app1-")
                    .build();

            collisionsEvents.sinkTo(sink);


        }catch (Exception e){
            e.printStackTrace();
        }


        env.execute(JOB_NAME);
    }

    private static Properties setDatabaseProperties(String conn_str, String username, String password, String table){
        Properties props = new Properties();
        props.setProperty("url",conn_str );
        props.setProperty("username", username);
        props.setProperty("password",password);
        props.setProperty("table",table);
        return props;
    }

    /** Map function to convert String to Json */
    private static JsonNode mapToJson(String jsonString) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readTree(jsonString);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
