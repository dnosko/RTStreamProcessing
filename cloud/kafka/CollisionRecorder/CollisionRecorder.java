package consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.reactivestreams.client.MongoDatabase;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;

import org.bson.Document;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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

        String mongoDbConnStr = config.getProperty("mongodb");
        String kafkaServer = config.getProperty("kafka_server");
        String kafkaGroup = config.getProperty("group_id_config");
        String topic = config.getProperty("topic_name");

        String securityprotocol = config.getProperty("security.protocol");
        String saslconfig = config.getProperty("sasl.jaas.config");
        String saslmechanism =  config.getProperty("sasl.mechanism");
        String clientdnslookup =  config.getProperty("client.dns.lookup");
        String sessionTimeout = config.getProperty("session.timeout.ms");

        final Properties consumer_props = new Properties();

        // Add additional properties.
        consumer_props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        consumer_props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroup);
        consumer_props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer_props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer_props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer_props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer_props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumer_props.put("security.protocol", securityprotocol);
        consumer_props.put("sasl.jaas.config", saslconfig);
        consumer_props.put("sasl.mechanism", saslmechanism);
        consumer_props.put("client.dns.lookup", clientdnslookup);
        consumer_props.put("session.timeout.ms", sessionTimeout);
        
        /**********************************************************/

        final Consumer<String, String> consumer = new KafkaConsumer<>(consumer_props);

        ConnectionString connString = new ConnectionString(mongoDbConnStr);
        // Set the Stable API version on the client.
        ServerApi serverApi = ServerApi.builder()
                .version(ServerApiVersion.V1)
                .build();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connString)
                .applyToConnectionPoolSettings(builder -> builder.maxSize(100))
                .applyToConnectionPoolSettings(builder ->
                        builder.maxWaitTime(5, TimeUnit.MINUTES))
                .serverApi(serverApi)
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase("db");
        MongoCollection<Document> collection = database.getCollection("collisions");
        //create indexes
        collection.createIndex(Indexes.ascending("device"));
        collection.createIndex(Indexes.compoundIndex(Indexes.ascending("device"), Indexes.ascending("polygon")));


        ObservableSubscriber<UpdateResult> subscriberUpdate = new ObservableSubscriber<UpdateResult>();
        ObservableSubscriber<InsertOneResult> subscriberInsert = new ObservableSubscriber<InsertOneResult>();

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
                            insertNewRecord(collection, jsonRecord, subscriberInsert);
                        }
                        else {
                            // exit polygon, update record
                            updateExistingRecord(collection, jsonRecord, subscriberUpdate);
                        }

                    }
                    // Commit the offsets of processed messages in this batch
                    if (!records.isEmpty())
                        consumer.commitSync();
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
                catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            consumer.close();
            mongoClient.close();
        }
    }

    /** Maps json point node to mongo point object */
    private static Point createGeoPoint(JsonNode point) {
        double x = point.get("x").asDouble();
        double y = point.get("y").asDouble();
        return new Point(new Position(x, y));
    }

    /** Inserts new collision to database. */
    private static void insertNewRecord(MongoCollection<Document> collection, JsonNode record, ObservableSubscriber<InsertOneResult> subscriberInsert) {
        long date_in = record.get("collision_date_in").asLong(); // convert from milli to micro seconds
        int polygon = record.get("polygon").asInt();
        int device =  record.get("device").asInt();
        boolean in = record.get("in").asBoolean();
        Timestamp date_ts = new Timestamp(date_in);

        Point geoPoint = createGeoPoint(record.get("collision_point_in"));

        Document document = new Document("device", device)
                .append("polygon", polygon)
                .append("inside", in)
                .append("collision_date_in", date_ts)
                .append("collision_point_in",geoPoint)
                .append("collision_point_out",null)
                .append("collision_date_out",null);

        collection.insertOne(document).subscribe(subscriberInsert);

    }

    /** Updates collision record when the device exits polygon. */
    private static void updateExistingRecord(MongoCollection<Document> collection, JsonNode record, ObservableSubscriber<UpdateResult> subscriberUpdate){
        long date_out = record.get("collision_date_out").asLong();
        int polygon = record.get("polygon").asInt();
        int device =  record.get("device").asInt();
        Point geoPoint = createGeoPoint(record.get("collision_point_out"));
        Timestamp date_ts = new Timestamp(date_out);

        // Criteria for the update
        Document filter = new Document("polygon", polygon)
                .append("device", device)
                .append("inside", true);

        // Specify the update operation
        Document update = new Document("$set", new Document("inside", false)
                .append("collision_date_out", date_ts)
                .append("collision_point_out", geoPoint));

        collection.updateOne(filter, update).subscribe(subscriberUpdate);
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
