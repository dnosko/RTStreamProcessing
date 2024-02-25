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
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.sedona.flink.SedonaContext;
import org.apache.sedona.flink.expressions.Constructors;
import org.apache.sedona.flink.expressions.Predicates;
import org.locationtech.jts.geom.Geometry;
import scala.collection.immutable.Stream;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Properties;
import org.apache.flink.table.api.Table;
import org.apache.sedona.common.utils.GeomUtils;
import static consumers.Utils.resultSetToCollection;
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

            // create a wkt table
            Table locationWktTable = Utils.createLocationsTable(sedona, jsonStream, locationColNames);
            //Constructors.ST_PolygonFromText
            //Constructors.ST_MakePoint
            //Constructors.ST_PointFromText
            //ST_

            Table pointTable = locationWktTable.select(
                    call(
                            new Constructors.ST_GeomFromWKT(),
                            $(locationColNames[1])
                    ).as(locationColNames[1]),
                    $(locationColNames[0])
            );
            /*pointTable = pointTable.select($(locationColNames[0]), $(locationColNames[1]),
                    call("ST_S2CellIDs", $(locationColNames[1]), 6).as("s2id_array"));*/
            //sedona.createTemporaryView("locationTable", pointTable);
            //pointTable = sedona.sqlQuery("SELECT geom_point, id_device, s2id_point FROM pointTable CROSS JOIN UNNEST(pointTable.s2id_array) AS tmpTbl1(s2id_point)");

            //Table locationsTable = sedona.sqlQuery("SELECT * FROM locationTable");
        /*pointTable.printSchema();
        pointTable.execute().print();*/

        try {
            Connection conn_db = DriverManager.getConnection(db_conn_string, db_username, db_password);
            //log.info("Connected to the database");
            String query = "SELECT id, creation, valid, ST_AsText(fence) as geo_fence FROM " + polygonsTable;
            try (Statement statement = conn_db.createStatement();
                 ResultSet resultSet = statement.executeQuery(query)) {

                Table polygonsWktTable = Utils.createPolygonsTable( sedona, resultSetToCollection(resultSet), polygonColNames);

                //sedona.createTemporaryView("polygonsTable", polygonsWktTable);
                //Table polygonsTable = sedona.sqlQuery("Select * from polygonsTable");
                //polygonsTable.execute().print();
                try {
                    Table polygonsTable = polygonsWktTable.select(call(Constructors.ST_GeomFromText.class.getSimpleName(),
                                    $(polygonColNames[1])).as(polygonColNames[1]),
                            $(polygonColNames[0]));
                    sedona.createTemporaryView("polygonTable", polygonsTable);
                    sedona.createTemporaryView("locationTable", pointTable);

                    Table joined = sedona.sqlQuery(
                            "SELECT * FROM locationTable, polygonTable"
                    );
                    joined.printSchema();

                    Table result = joined
                            .select($("*"),call("ST_Contains", $(locationColNames[1]), $(polygonColNames[1])).as("contains"));
                    //result = result.where($("contains").isTrue()).select($(locationColNames[0]), $(polygonColNames[0]));
                    result = result.select($(locationColNames[0]), $(polygonColNames[0]), $("contains"));
                    result.printSchema();
                    result.execute().print();



                    // Create S2CellID
                    /*polygonsTable = polygonsTable.select($(polygonColNames[0]), $(polygonColNames[1]),
                            call("ST_S2CellIDs", $(polygonColNames[1]), 6).as("s2id_array"));
                    // Explode s2id array
                    sedona.createTemporaryView("polygonTable", polygonsTable);
                    polygonsTable = sedona.sqlQuery("SELECT geom_polygon, id_polygon, s2id_polygon FROM polygonTable CROSS JOIN UNNEST(polygonTable.s2id_array) AS tmpTbl2(s2id_polygon)");

                    Table joinResult = pointTable.join(polygonsTable).where($("s2id_point").isEqual($("s2id_polygon")));
                    // Optional: remove false positives
                    String expr = "ST_Contains(geom_polygon, geom_point)";
                    joinResult = joinResult.filter(expr);
                    joinResult.execute().print();*/

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
         *  2. zobrat polohu a vypocitat do akeho polygonu patri - premysliet akym sposobom..
         *  3. map reduce maybe?
         *   Apache Sedona alebo GEOFLINK?
         *   vyuzit a zmenit format sprav ktore sa posielaju do kafka tj. GEOJSON asi?...
         *   to znamena ze prerobit producera, location tracker consumera a mozno aj questDB :(
         *  {
         *   "type": "Feature",
         *   "geometry": {
         *     "type": "Point",
         *     "coordinates": [139.8107, 35.7101]
         *   },
         *   "properties": {
         *     "name": "Tokyo Skytree"
         *   }
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
