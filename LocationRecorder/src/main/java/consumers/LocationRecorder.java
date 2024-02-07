// adapted from: https://developer.confluent.io/get-started/java/#build-consumer
package consumers;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPooled;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocationRecorder {

    public static void main(final String[] args) throws Exception {

        final String topic = "new_locations";
        JedisPooled jedis = new JedisPooled("redis", 6379);
        AtomicInteger cnt = new AtomicInteger(0);

        final Properties props = new Properties();

        // Add additional properties.
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:19092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-location-recorder");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final Consumer<String, String> consumer = new KafkaConsumer<>(props);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        try {
            consumer.subscribe(Arrays.asList(topic));

            scheduler.scheduleAtFixedRate(() -> {
                // Perform your action, for example, write to stdout
                System.out.println(cnt.get());
                cnt.set(0);
            }, 0, 1, TimeUnit.MINUTES);

            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                            // Process the record
                            String key = record.key();
                            String value = record.value();
                            writeToRTDB(jedis, key, value);
                            cnt.incrementAndGet();
                            //System.out.println(cnt);

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
        System.out.println(jedis.get(key));
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
