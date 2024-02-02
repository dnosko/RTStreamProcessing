// adapted from: https://developer.confluent.io/get-started/java/#build-consumer
package consumers;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPooled;

public class LocationRecorder {

    public static void main(final String[] args) throws Exception {

        final String topic = "new_locations";
        JedisPooled jedis = new JedisPooled("localhost", 6379);
        int cnt = 0;

        // Load consumer configuration settings from a local file
        // Reusing the loadConfig method from the ProducerExample class
        final Properties props = new Properties();

        // Add additional properties.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-location-recorder");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    writeToRTDB(jedis, key, value);
                    cnt++;
                    System.out.println(cnt);
                    /*System.out.println(
                            String.format("Consumed event from topic %s: key = %-10s value = %s", topic, key, value));*/
                }
            }
        }
    }

    static void writeToRTDB(JedisPooled jedis, String key, String value){
        jedis.set(key, value);
        System.out.println(jedis.get(key));
    }


}
