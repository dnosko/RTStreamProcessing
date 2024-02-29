package consumers;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public interface TableFactory<T, U> {
    /** Creates new table in stream environment with given colNames.
    * Types of columns are specified inside concrete functions
    **/
    Table createTable(StreamTableEnvironment tableEnv, T data, String[] colNames);
    /** Creates Geometry column for given object and selects needed columns */
    Table createGeometryTable(String[] colNames, Table sourceTable);
    /** Creates row for flink table */
    Row createWKTGeometry(U object);
}
