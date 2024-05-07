/** Daša Nosková - xnosko05
 *  VUT FIT 2024
 * Body of consumer adapted from: https://developer.confluent.io/get-started/java/#build-consumer
 */


package consumers;

import org.apache.kafka.clients.consumer.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import redis.clients.jedis.JedisPooled;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocationRecorder {

    public static void main(final String[] args) throws Exception {

        Properties config = new Properties();
        String fileName = "app.config";
        try (FileInputStream fis = new FileInputStream(fileName)) {
            config.load(fis);
        } catch (FileNotFoundException ex) {
                System.out.println("Config file not found.");
        }

        /************************ Config ***************************/

        String redisHost = config.getProperty("redis_host");
        int redisPort = Integer.parseInt(config.getProperty("redis_port"));
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

        JedisPooled jedis = new JedisPooled(redisHost, redisPort );


        final Consumer<String, String> consumer = new KafkaConsumer<>(consumer_props);

        try {
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                            // Process the record
                            String key = record.key();
                            String value = record.value();
                            writeToRTDB(jedis, key, value);
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
        finally {
            consumer.close();
        }
    }

    static void writeToRTDB(JedisPooled jedis, String key, String value){
        jedis.set(key, value);
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
            // different exception
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
            // different exception
            return -1;
        }
    }

}
