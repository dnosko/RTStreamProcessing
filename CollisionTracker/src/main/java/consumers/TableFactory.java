package consumers;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public interface TableFactory<T, U> {
    Table createTable(StreamTableEnvironment tableEnv, T data, String[] colNames);
    Table createGeometryTable(String[] colNames, Table sourceTable);
    Row createWKTGeometry(U object);
}
