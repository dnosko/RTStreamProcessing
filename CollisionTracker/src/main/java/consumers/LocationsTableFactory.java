/** Daša Nosková - xnosko05
 *  VUT FIT 2024
 **/

package consumers;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.sedona.flink.expressions.Constructors;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class LocationsTableFactory implements TableFactory<DataStream<JsonNode>,JsonNode>, Serializable {

    /** Creates table from json stream of data and adds processing time timestamps (watermarks)
     *
     *  Modified from Apache Sedona originally licensed under Apache License 2.0
     * 
     *  See the NOTICE-sedona and LICENSE-2.0 files at the CollisionTracker/license for additional attributions.
     *  Based on: https://github.com/apache/sedona/blob/master/examples/flink-sql/src/main/java/Utils.java#L100/
     * */
    @Override
    public Table createTable(StreamTableEnvironment tableEnv, DataStream<JsonNode> data, String[] colNames) {
        TypeInformation<?>[] colTypes = {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        };
        RowTypeInfo typeInfo = new RowTypeInfo(colTypes, Arrays.copyOfRange(colNames, 0, 3));
        DataStream<Row> ds = data.map(s -> createWKTGeometry(s)).returns(typeInfo);
        // Generate Time Attribute
        WatermarkStrategy<Row> wmStrategy =
                WatermarkStrategy
                        .<Row>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getFieldAs(2));

        return tableEnv.fromDataStream(ds.assignTimestampsAndWatermarks(wmStrategy), $(colNames[0]), $(colNames[1]), $(colNames[2]).rowtime(), $(colNames[3]).proctime());
    }

    /** Creates Geometry column for point
     *  Selects all columns from the original table.
     * */
    @Override
    public Table createGeometryTable(String[] colNames, Table sourceTable) {
        return sourceTable.select(
                call(new Constructors.ST_GeomFromWKT(), $(colNames[1])).as(colNames[1]),
                $(colNames[0]),
                $(colNames[2]),
                $(colNames[3])
        );
    }

    /** Creates a row in table from JsonNode which has format:
     * {'id': value, 'point': {'x': value, 'y': value}, 'timestamp:' value}
     * Converts original timestamp which is in microseconds to milliseconds.
     * */
    @Override
    public Row createWKTGeometry(JsonNode node) {
        JsonNode pointNode = node.get("point");
        Long timestampInMs = node.get("timestamp").asLong() / 1000; // convert from micro to milli seconds

        Row row = Row.of(node.get("id").asInt(), "POINT (" + pointNode.get("x").asDouble() + " " + pointNode.get("y").asDouble() +")", timestampInMs );
        return row;
    }
}
