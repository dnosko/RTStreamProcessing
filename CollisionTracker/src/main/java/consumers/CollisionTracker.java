package consumers;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import lombok.extern.slf4j.Slf4j;
@Slf4j
public class CollisionTracker {
    static String JOB_NAME = "Collision Tracker";

    public static void main(final String[] args) throws  Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics("new_locations")
        .setGroupId("collision-tracker")
                .setProperty("partition.discovery.interval.ms", "10000") // Dynamic Partition Discovery for scaling out topics
        .setStartingOffsets(OffsetsInitializer.earliest()) // neskor zmenit na OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        stream.print();
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
}
