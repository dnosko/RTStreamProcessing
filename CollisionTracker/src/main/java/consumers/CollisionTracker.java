package consumers;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.types.Row;
import org.apache.sedona.flink.SedonaContext;
import org.apache.sedona.flink.expressions.Functions;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.*;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.*;

@Slf4j
public class CollisionTracker {
    static String JOB_NAME = "Collision Tracker";
    public static final int CHECKPOINTING_INTERVAL_MS = 5000;
    static final int ERR_DB = -2;
    static final String polygonsTable = "polygons";
    static final String inputTopic = "new_locations";

    static String[] locationColNames = {"id_device", "geom_point", "timestamp", "processing_time"};
    static String[] polygonColNames = {"id_polygon","geom_polygon", "valid", "creation"};

    public static void main(final String[] args) throws  Exception {

        /************************************ SETUP ********************************************/

        Properties config = new Properties();
        String fileName = "app.config";
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
        // in the future for more devices maybe consider rocksDB https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/state/state_backends/
        env.setStateBackend(new HashMapStateBackend());
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        StreamTableEnvironment sedona = SedonaContext.create(env, tableEnv);

        // TODO prerobit na priamo tabulka z kafka? skusit..
        // https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/table/overview/
        KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(kafkaServer)
        .setTopics(inputTopic)
        .setGroupId(groupID)
        .setProperty("partition.discovery.interval.ms", "10000") // Dynamic Partition Discovery for scaling out topics
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

        try {
            // initialize tables
            Table polygonsWktTable = polygonsFactory.createTable(sedona, databaseProps, polygonColNames);
            Table locationWktTable = locationsFactory.createTable(sedona, jsonStream, locationColNames);

            // create tables with geometry columns
            Table locationsTable = locationsFactory.createGeometryTable(locationColNames, locationWktTable);
            Table polygonsTable = polygonsFactory.createGeometryTable(polygonColNames, polygonsWktTable);

            sedona.createTemporaryView("polygonTable", polygonsTable);
            sedona.createTemporaryView("locationTable", locationsTable);

            //Table result = polygonsTable.select(call(new Functions.ST_AsText(), $(polygonColNames[1])).as(polygonColNames[1]), $(polygonColNames[0]));
            //Table result = locationsTable.select(call(new Functions.ST_AsText(), $(locationColNames[1])).as(locationColNames[1]), $(locationColNames[0]));
            //result.execute().print();


            Table joined = sedona.sqlQuery("SELECT *, " +
                    "ST_Contains(" + polygonColNames[1] + "," + locationColNames[1] +") AS is_in_polygon "+
                    "FROM locationTable, polygonTable");

            // it's enough to keep only id of polygon at this point, the geometry isn't needed anymore, for locations all columns are needed
            Table dropPolygonCol = joined.dropColumns($(polygonColNames[1]));
            //dropPolygonCol.printSchema();
            DataStream<Row> resultStream = sedona.toDataStream(dropPolygonCol);
            //resultStream.print();
            // group stream by device_id,
            //resultStream.keyBy(r -> r.getField(locationColNames[0]).toString()).print();
            //TODO neviem ci to nespracovava duplikatne zaznamy zo streamu... rozne thready
            DataStream<String> collisionsEvents = resultStream.keyBy(r -> (Integer) r.getField(1))
                                .flatMap(new PolygonMatchingFlatMap()).map(e -> e.toString());
            collisionsEvents.print();


        }catch (Exception e){
            e.printStackTrace();
        }


        env.execute(JOB_NAME);
        /** TODO
         *  2. Zobrat stream dat zgrupit podla kluca zariadenia  a udrziavat si nejaku celkovu stavovu pamat,
         *      ze ci je alebo nie je v polygone s danim ID ( posledny stlpec => contains).
         *  3. zaznamenat tento vypocet do stavovej pamate (podla klucu?) alebo mozno normalne
         *  4. porovnat to s predchadzajucim a na zaklade toho vygenerovat udalost a sink do dvoch topikov v kafka.
         */


        //
        // 3. porovnat zmenu -> stavovost pre kazde ID zariadenia
        // 4a. vytvorit novy topic
        // 4. vyegenerovat prislusnnu udalost do topicu
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
