package consumers;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.sedona.flink.SedonaContext;
import org.apache.sedona.flink.expressions.Constructors;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

@Slf4j
public class CollisionTracker {
    static String JOB_NAME = "Collision Tracker";
    public static final int CHECKPOINTING_INTERVAL_MS = 5000;

    public static void main(final String[] args) throws  Exception {

        Properties config = new Properties();
        String fileName = "app.config";
        try (FileInputStream fis = new FileInputStream(fileName)) {
            config.load(fis);
        } catch (FileNotFoundException ex) {
            System.out.println("Config file not found.");
        }

        String kafkaServer = config.getProperty("kafka_server");
        String topic = config.getProperty("topic");
        String groupID = config.getProperty("group_id");

        // set up environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        StreamTableEnvironment sedona = SedonaContext.create(env, tableEnv);

        // set consuming messages from kafka topic
        KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(kafkaServer)
        .setTopics(topic)
        .setGroupId(groupID)
        .setProperty("partition.discovery.interval.ms", "10000") // Dynamic Partition Discovery for scaling out topics
        .setStartingOffsets(OffsetsInitializer.earliest()) // neskor zmenit na OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

        // create stream
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<JsonNode> jsonStream = stream.map(CollisionTracker::mapToJson);

        String[] colNames = {"id", "geom_point", "timestamp", "processing_time"};


        // create a wkt table
        Table pointWktTable = Utils.createTable(env, sedona, jsonStream, colNames);
        // Create a geometry column
        /*Table locationTable = pointWktTable.select(call(Constructors.ST_GeomFromWKT.class.getSimpleName(),
                        $(colNames[1])).as(colNames[1]),
                $(colNames[0]));*/

        tableEnv.createTemporaryView("locationTable", pointWktTable);
        pointWktTable = tableEnv.sqlQuery("SELECT * FROM locationTable");
        pointWktTable.execute().print();



        // create table from new location stream
        // TODO najprv namapovat milisekundy na stringovy datetime a ulozit ako timestamp?
        /*TableResult locations = tableEnv.executeSql(
                "CREATE TABLE locations " +
                        "(`id` BIGINT," +
                        "`x` DOUBLE," +
                        "`y` DOUBLE," +
                        "`ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'" +
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'new_locations'," +
                        "'properties.bootstrap.servers' = 'localhost:9092'," +
                        "'properties.group.id' = 'testGroup',"+
                        "'scan.startup.mode' = 'earliest-offset'," +
                        "'format' = 'json'," +
                        "'json.ignore-parse-errors' = 'true'," +
                        "'key.fields' = 'id'," +
                        "'key.format' = 'json'," +
                        "'value.fields-include' = 'ALL')" // Since the value format is configured with 'value.fields-include' = 'ALL', key fields will also end up in the value format’s data type
        );*/


        //Table locations = tableEnv.fromDataStream(jsonStream).as();
        //tableEnv.createTemporaryView("locations", locations);

        //Table result = locations.select($("*"));
        //resultStream.print();
        //Table resultTable = tableEnv.sqlQuery("SELECT * FROM locations;");

        //resultTable.printSchema();



        //jsonStream.print();

        env.execute(JOB_NAME);
        /** TODO
         *  1. pripojenie do DB a natiahnut si polygony
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
