package consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TODO zobrat consumera s locationRecorder
// QuestDB connect jbdc
// podla typu udalosti insertnut alebo updatnut hodnotu v DB
public class CollisionRecorder {

    static int geoHashCharSize = 10;

    // 1 char = 5 bits. https://questdb.io/docs/concept/geohashes/
    static int geoHashBitSize = geoHashCharSize*5;

    public static void main(final String[] args) throws Exception {

        Properties config = new Properties();
        String fileName = "app.config";
        try (FileInputStream fis = new FileInputStream(fileName)) {
            config.load(fis);
        } catch (FileNotFoundException ex) {
            System.out.println("Config file not found.");
        }

        /************************ Config ***************************/

        String questUrl = config.getProperty("quest_db");
        String quest_data = config.getProperty("quest_data");
        String kafkaServer = config.getProperty("kafka_server");
        String kafkaGroup = config.getProperty("group_id_config");
        String topic = config.getProperty("topic_name");

        final Properties consumer_props = new Properties();

        // Add additional properties.
        consumer_props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        consumer_props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroup);
        consumer_props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer_props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer_props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /**********************************************************/

        //TODO questDB conneciton

        //JedisPooled jedis = new JedisPooled(redisHost, redisPort ); // redis
        //AtomicInteger cnt = new AtomicInteger(0);

        final Consumer<String, String> consumer = new KafkaConsumer<>(consumer_props);
        final CairoConfiguration configuration = new DefaultCairoConfiguration(quest_data);
        System.out.println(configuration.getAllowTableRegistrySharedWrite());

        try (CairoEngine engine = new CairoEngine(configuration)){
            //Connection connection = DriverManager.getConnection(questUrl);
            final SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(AllowAllSecurityContext.INSTANCE, null);
            System.out.println("here");
            engine.ddl("CREATE TABLE IF NOT EXISTS collisions_table (" +
                    "id INT AUTO_INCREMENT, device int, polygon int, in boolean, collision_date_in timestamp, collision_date_out timestamp, " +
                    "collision_point_in geohash("+geoHashCharSize+"c), collision_point_out geohash("+geoHashCharSize+"c)" +
                    ") TIMESTAMP(collision_date_in) PARTITION BY DAY WAL", ctx);

            final TableToken tableToken = engine.getTableTokenIfExists("collisions");

            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        //TODO spracovanie recordu na zaklade typu insert alebo update
                        // Process the record

                        String key = record.key();
                        String value = record.value();
                        JsonNode jsonRecord = mapToJson(value);
                        String typeOfRecord = jsonRecord.get("event_type").asText();
                        if (typeOfRecord.equals("enter")){
                            insertNewRecord(engine, tableToken, jsonRecord);
                            // apply WAL to the table
                            try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 1, 1)) {
                                while (walApplyJob.run(0)) ;
                            }
                        }
                        else {
                            try {
                                long rowId = getRowId(engine,ctx, jsonRecord);
                                updateExistingRecord(engine,tableToken,jsonRecord, rowId);
                            }
                            catch (SqlException e) {
                                e.printStackTrace();
                                System.out.println("Couldn't update row.");
                            }
                        }
                    }
                } catch (KafkaException e) {
                    System.out.println(e.getCause());
                    long offset = getOffset(e.getCause().toString());
                    System.out.println(offset);

                    if (offset != -1) {
                        int partition = getPartition(e.getCause().toString(), topic);
                        System.out.println(partition);
                        TopicPartition partitionToSeek = new TopicPartition(topic, partition);
                        consumer.seek(partitionToSeek, offset);
                    }
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            consumer.close();
        }
    }


    private static void insertNewRecord(CairoEngine engine, TableToken tableToken, JsonNode record){
        try (WalWriter writer = engine.getWalWriter(tableToken)) {
                long geoHashPointIn = getGeoHashFromJson("collision_point_in", record);
                TableWriter.Row row = writer.newRow();
                row.putInt(0, record.get("device").asInt());
                row.putInt(1, record.get("polygon").asInt());
                row.putBool(2, record.get("in").asBoolean());
                row.putTimestamp(3, record.get("collision_date_in").asLong());
                row.putTimestamp(4, 0L);
                row.putGeoHash(5, geoHashPointIn);
                //row.putGeoHash(6, null);
                row.append();
            writer.commit();
        }
        catch (NumericException e){
            e.printStackTrace();
        }
    }

    private static long getRowId(CairoEngine engine,SqlExecutionContext ctx, JsonNode record) throws SqlException {
        int device = record.get("device").asInt();
        int polygon = record.get("polygon").asInt();

        // Perform a select query to retrieve the id of the row to update
        String query = "SELECT id FROM your_table WHERE in = true and device = " + device + " AND polygon = " + polygon;
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                final Record result = cursor.getRecord();
                return result.getRowId();
            }
        }
    }

    private static void updateExistingRecord(CairoEngine engine, TableToken tableToken, JsonNode record, Long rowId){
        try (WalWriter writer = engine.getWalWriter(tableToken)) {
            long geoHashPointOut = getGeoHashFromJson("collision_point_out", record);

            TableWriter.Row row = writer.newRow(rowId);
            row.putBool(2, false); // Update in (polygon) to false (exit event)
            row.putTimestamp(4, record.get("collision_date_out").asLong());
            row.putGeoHash(6, geoHashPointOut);
            row.append();
            writer.commit();
        } catch (NumericException e) {
            e.printStackTrace();
        }
    }

    private static long getGeoHashFromJson(String fieldName, JsonNode node) throws NumericException {
        JsonNode point = node.get(fieldName);
        double lat = point.get("x").asDouble();
        double lng = point.get("y").asDouble();
        return GeoHashes.fromCoordinatesDeg(lat, lng,geoHashBitSize);
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

    /* Extract offset of corrupted record. If its a different kind of exception, then returns -1*/
    static long getOffset(String msg){
        String patternString = "stored crc" + " = (\\d+)";
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(msg);

        if (matcher.find()) {
            // return offset value
            return Long.parseLong(matcher.group(1));
        } else {
            return -1;
        }
    }

    /*Extract partition number when throwing corrupted record exception */
    static int getPartition(String msg, String topicName){
        String patternString = topicName + "-(\\d+)";
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(msg);

        if (matcher.find()) {
            // return offset value
            return Integer.parseInt(matcher.group(1));
        } else {
            return -1;
        }
    }

}
