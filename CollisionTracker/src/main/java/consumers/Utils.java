package consumers;

import lombok.extern.slf4j.Slf4j;
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
import org.locationtech.jts.io.ParseException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.apache.flink.table.api.Expressions.$;

/*** Based on: https://github.com/apache/sedona/blob/master/examples/flink-sql/src/main/java/Utils.java#L71/*/
@Slf4j
public class Utils {

    /* Creates table from json stream of data and adds processing time timestamps (watermarks) */
    static Table createLocationsTable(StreamTableEnvironment tableEnv, DataStream<JsonNode> jsonStream, String[] colNames){
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

    static Table createPolygonsTable(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, List<Row> data, String[] colNames){
        TypeInformation<?>[] colTypes = {
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.BOOLEAN_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        };

        RowTypeInfo typeInfo = new RowTypeInfo(colTypes, Arrays.copyOfRange(colNames, 0, 4));
        DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);

        return tableEnv.fromDataStream(ds, $(colNames[0]), $(colNames[1]), $(colNames[2]), $(colNames[3]) );
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

    static Row createPolygonWKT(Polygon polygon) throws SQLException, ParseException {
        Row row = Row.of(polygon.id, polygon.fence, polygon.valid, polygon.creation.getTime() );
        return row;
    }

    // Helper method to convert ResultSet from database to collection of class Polygon
    static List<Row> resultSetToCollection(ResultSet resultSet) throws SQLException {
        int id;
        String fence;
        boolean valid;
        Date creation;
        List<Row> dataList = new ArrayList<>();
        while (resultSet.next()) {
            id = resultSet.getInt("id");
            fence  = resultSet.getString("geo_fence");
            System.out.println(fence);
            valid  = resultSet.getBoolean("valid");

            // skip not valid polygons
            if (!valid)
                continue;

            creation  = resultSet.getTimestamp("creation");

            Polygon poly = new Polygon(id,fence,valid,creation );
            try {
                dataList.add(createPolygonWKT(poly));
            }
            catch (ParseException e) {
                System.out.println(e);
            }
        }
        return dataList;
    }

}
