package consumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.questdb.std.NumericException;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.sql.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CollisionRecorder {

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

        final Consumer<String, String> consumer = new KafkaConsumer<>(consumer_props);

        Properties questProps = new Properties();
        questProps.setProperty("user", "admin");
        questProps.setProperty("password", "quest");
        questProps.setProperty("sslmode", "disable");

        final Connection connection = DriverManager.getConnection(
                questUrl, questProps);
        connection.setAutoCommit(false);


        // create table
        final PreparedStatement statement = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS collisions_table (" +
                        "device int, polygon int, inside boolean, collision_date_in timestamp, collision_date_out timestamp , " +
                        "collision_point_in STRING, collision_point_out STRING" +
                        ") TIMESTAMP(collision_date_in) PARTITION BY DAY WAL;");
        statement.execute();


        try {
            consumer.subscribe(Arrays.asList(topic));
            System.out.println("Collision Recorder started.");

            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {

                        String value = record.value();
                        JsonNode jsonRecord = mapToJson(value);
                        String typeOfRecord = jsonRecord.get("event_type").asText();


                        if (typeOfRecord.equals("enter")){ // enter polygon, create new record
                            insertNewRecord(connection, jsonRecord);
                        }
                        else {
                            // exit polygon, update record
                            updateExistingRecord(connection, jsonRecord);
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

    /** Maps json inner dictionary to string */
    private static String mapPointToString(JsonNode jsonNode) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(jsonNode);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    /** Inserts new collision to database. */
    private static void insertNewRecord(Connection connection, JsonNode record) throws NumericException {
        long date_in = record.get("collision_date_in").asLong(); // convert from milli to micro seconds
        int polygon = record.get("polygon").asInt();
        int device =  record.get("device").asInt();
        boolean in = record.get("in").asBoolean();
        Timestamp date_ts = new Timestamp(date_in);
        String point_in = mapPointToString(record.get("collision_point_in"));

        try (PreparedStatement preparedStatement = connection.prepareStatement(
                "INSERT INTO collisions_table (device, polygon, inside, collision_date_in, collision_point_in) VALUES (?, ?, ?, ?, ?)")) {
            preparedStatement.setInt(1, device);
            preparedStatement.setInt(2, polygon);
            preparedStatement.setBoolean(3, in);
            preparedStatement.setTimestamp(
                    4,date_ts);
            preparedStatement.setString(5, point_in);
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** Updates collision record when the device exits polygon. */
    private static void updateExistingRecord(Connection connection, JsonNode record){
        long date_out = record.get("collision_date_out").asLong();
        int polygon = record.get("polygon").asInt();
        int device =  record.get("device").asInt();
        String point_out = mapPointToString(record.get("collision_point_out"));

        String updateSql = "UPDATE collisions_table SET " +
                "collision_date_out = ?, " +
                "collision_point_out = ?, " +
                "inside = ? " +
                "WHERE polygon = ? AND device = ? AND inside = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(updateSql)) {
            pstmt.setTimestamp(1, new Timestamp(date_out));
            pstmt.setString(2, point_out);
            pstmt.setBoolean(3, false);
            pstmt.setInt(4, polygon);
            pstmt.setInt(5, device);
            pstmt.setBoolean(6, true);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
