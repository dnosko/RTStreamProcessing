package consumers;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.types.Row;
import org.apache.sedona.flink.SedonaContext;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.*;

@Slf4j
public class CollisionTracker {
    static String JOB_NAME = "Collision Tracker";
    public static final int CHECKPOINTING_INTERVAL_MS = 1000;
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
        String groupID = config.getProperty("group_id");
        String db_conn_string = config.getProperty("postgres_connection_string");
        String db_username = config.getProperty("postgres_username");
        String db_password = config.getProperty("postgres_password");
        Properties databaseProps = setDatabaseProperties(db_conn_string, db_username, db_password, polygonsTable);



        // set up flink's stream environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(CHECKPOINTING_INTERVAL_MS);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINTING_INTERVAL_MS*4);
        // in the future for more devices maybe consider rocksDB https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/state/state_backends/
        env.setStateBackend(new HashMapStateBackend());
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        StreamTableEnvironment sedona = SedonaContext.create(env, tableEnv);

        String uniqueSuffix = Long.toString(System.currentTimeMillis());
        // TODO prerobit na priamo tabulka z kafka? skusit..
        // https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/table/overview/
        KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(kafkaServer)
        .setTopics(inputTopic)
        .setGroupId("collision-tracker")
        .setClientIdPrefix(uniqueSuffix)
        .setProperty("partition.discovery.interval.ms", "10000") // Dynamic Partition Discovery for scaling out topics
        .setProperty("register.consumer.metrics", "true")
        .setStartingOffsets(OffsetsInitializer.earliest()) // neskor zmenit na OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

        /**************************************************************************************/

        // create stream
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<JsonNode> jsonStream = stream.map(CollisionTracker::mapToJson);

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

            /*
            * Please ensure that you use unique transactionalIdPrefix across your applications running
            *  on the same Kafka cluster such that multiple running jobs do not interfere in their transactions!
            * Additionally, it is highly recommended to tweak Kafka transaction timeout
            * (see Kafka producer transaction.timeout.ms)» maximum checkpoint duration + maximum restart duration
            * or data loss may happen when Kafka expires an uncommitted transaction.
            * */

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
