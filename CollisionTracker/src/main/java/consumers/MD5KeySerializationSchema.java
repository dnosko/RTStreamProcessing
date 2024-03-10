import org.apache.flink.streaming.connectors.kafka.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.serialization.KafkaSerializationSchema;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5KeySerializationSchema implements KafkaSerializationSchema<String> {

    @Override
    public byte[] serialize(String element, @Nullable Long timestamp) {
        return generateMD5(element).getBytes(StandardCharsets.UTF_8);
    }

    private String generateMD5(String data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(data.getBytes());
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }
}
