package consumers;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/*** Based on: https://github.com/apache/sedona/blob/master/examples/flink-sql/src/main/java/Utils.java#L71/*/
public class Utils {

    /* Creates table from json stream of data and adds processing time timestamps (watermarks) */
    static Table createTable(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, DataStream<JsonNode> jsonStream, String[] colNames){
        TypeInformation<?>[] colTypes = {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        };
        RowTypeInfo typeInfo = new RowTypeInfo(colTypes, Arrays.copyOfRange(colNames, 0, 3));
        DataStream<Row> ds = jsonStream.map(s -> createPointWKT(s)).returns(typeInfo);
        // Generate Time Attribute
        WatermarkStrategy<Row> wmStrategy =
                WatermarkStrategy
                        .<Row>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getFieldAs(2));
        return tableEnv.fromDataStream(ds.assignTimestampsAndWatermarks(wmStrategy), $(colNames[0]), $(colNames[1]), $(colNames[2]).rowtime(), $(colNames[3]).proctime());
    }

    /* Creates a row in table from JsonNode which has format:
    * {'id': value, 'point': {'x': value, 'y': value}, 'timestamp:' value}
    * Converts original timestamp which is in microseconds to milliseconds.
    * */
    static Row createPointWKT(JsonNode node){
        JsonNode pointNode = node.get("point");
        Long timestampInMs = node.get("timestamp").asLong() / 1000; // convert from micro to milli seconds

        Row row = Row.of(node.get("id").asInt(), "POINT (" + pointNode.get("x").asDouble() + " " + pointNode.get("y").asDouble() +")", timestampInMs );
        return row;
    }

}
