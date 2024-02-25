package consumers;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.sedona.flink.SedonaContext;
import org.apache.sedona.flink.expressions.Functions;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.List;
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


        // set up flink's stream environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        StreamTableEnvironment sedona = SedonaContext.create(env, tableEnv);

        // set consuming messages from kafka topic
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

        try {
            Connection conn_db = DriverManager.getConnection(db_conn_string, db_username, db_password);
            //log.info("Connected to the database");
            String query = "SELECT id, creation, valid, ST_AsText(fence) as geo_fence FROM " + polygonsTable;
            try (Statement statement = conn_db.createStatement();
                 ResultSet resultSet = statement.executeQuery(query)) {

                TableFactory<DataStream<JsonNode>, JsonNode> locationsFactory = new LocationsTableFactory();
                TableFactory<ResultSet, Polygon> polygonsFactory = new PolygonsTableFactory();

                // create tables for polygons and incoming locations
                Table polygonsWktTable = polygonsFactory.createTable(sedona, resultSet, polygonColNames);
                Table locationWktTable = locationsFactory.createTable(sedona, jsonStream, locationColNames);

                try {

                    Table locationsTable = locationsFactory.createGeometryTable(locationColNames, locationWktTable);
                    Table polygonsTable = polygonsFactory.createGeometryTable(polygonColNames, polygonsWktTable);

                    sedona.createTemporaryView("polygonTable", polygonsTable);
                    sedona.createTemporaryView("locationTable", locationsTable);

                    Table joined = sedona.sqlQuery(
                            "SELECT * FROM locationTable, polygonTable"
                    );
                    //joined.printSchema();

                    Table result = joined
                            .select($("*"),call("ST_Contains", $(polygonColNames[1]), $(locationColNames[1])).as("contains"));

                    result = result.select($("contains"),
                            call(new Functions.ST_AsText(), $(polygonColNames[1])).as(polygonColNames[1]),
                            call(new Functions.ST_AsText(), $(locationColNames[1])).as(locationColNames[1]),
                            $(polygonColNames[0]), $(locationColNames[0]), $(locationColNames[2])).where($("contains").isTrue());

                    result.execute().print();


                }catch (Exception e){
                    e.printStackTrace();
                }
            } catch (SQLException e) {
                //log.error("Error executing SQL query: {}", e.getMessage());
                System.out.println(e.getMessage());
            }
        }
        catch (SQLException e) {
            //log.error("Failed to connect to the database: {}", e.getMessage());
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.exit(ERR_DB);
        }

        env.execute(JOB_NAME);
        /** TODO
         *
         *  3. zaznamenat tento vypocet do stavovej pamate (podla klucu?) alebo mozno normalne
         *  4. porovnat to s predchadzajucim a na zaklade toho vygenerovat udalost a sink do dvoch topikov v kafka.
         */


        //
        // 2. Vypocitat do akych polygonov bod patri -> todo DB s polygonmi nasadit
        // 3. porovnat zmenu -> stavovost pre kazde ID zariadenia
        // 4a. vytvorit novy topic
        // 4. vyegenerovat prislusnnu udalost do topicu
    }

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
