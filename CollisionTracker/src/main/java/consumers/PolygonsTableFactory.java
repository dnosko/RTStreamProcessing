/** Daša Nosková - xnosko05
 *  VUT FIT 2024
 **/

package consumers;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.sedona.flink.expressions.Constructors;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.*;

public class PolygonsTableFactory implements TableFactory<Properties, Polygon> {

    /** Creates flink table of polygons retrieved from database.
    * id_polygon = int
    * geom_polygon = string (wkt format)
    * valid = boolean (whether polygon is valid)
    * creation = timestamp
    * */
    @Override
    public Table createTable(StreamTableEnvironment tableEnv, Properties properties, String[] colNames) {
        List<Row> data = getPolygonsFromDatabase(properties);
        Table table = tableEnv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD(colNames[0], DataTypes.INT()),
                        DataTypes.FIELD(colNames[1], DataTypes.STRING()),
                        DataTypes.FIELD(colNames[2], DataTypes.BOOLEAN()),
                        DataTypes.FIELD(colNames[3], DataTypes.TIMESTAMP())
                ),
                data
        );

        return table;
    }

    /** Creates Geometry column for polygons that are valid (column 3, therefor 2 in array..).
    *  Selects only columns: geom_polygon and id_polygon
    * */
    @Override
    public Table createGeometryTable(String[] colNames, Table sourceTable) {
        Table validOnly = filterValidPolygons(sourceTable, colNames[2]);
        return validOnly.select(
                call(new Constructors.ST_GeomFromWKT(), $(colNames[1])).as(colNames[1]),
                $(colNames[0])
        );
    }

    /** Creates table row of polygon for flink table */
    @Override
    public Row createWKTGeometry(Polygon polygon) {
        Row row = Row.of(polygon.id, polygon.fence, polygon.valid, polygon.creation );
        return row;
    }

    /** Returns only polygons that are valid */
    private static Table filterValidPolygons(Table table, String valid){
        return table.filter($(valid).isTrue());
    }

    /** Helper method to convert ResultSet from database to collection of rows using class Polygon */
    private List<Row> resultSetToCollection(ResultSet resultSet) {
        int id;
        String fence;
        boolean valid;
        Date creation;
        List<Row> dataList = new ArrayList<>();
        try{
        while (resultSet.next()) {
            id = resultSet.getInt("id");
            fence  = resultSet.getString("geo_fence");
            valid  = resultSet.getBoolean("valid");

            creation  = resultSet.getTimestamp("creation");

            Polygon poly = new Polygon(id,fence,valid,creation );
            dataList.add(createWKTGeometry(poly));

        }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return dataList;
    }

    /** Gets polygons from database and returns them as rows for flink table.
     * Properties are expected to have format:
     *      url: connection string
     *      username
     *      password
     *      table: name of polygons table
     * In case of exception program ends.
     * */
    private  List<Row> getPolygonsFromDatabase(Properties databaseProps){
            try {
                Connection conn_db = DriverManager.getConnection(
                        databaseProps.getProperty("url"),
                        databaseProps.getProperty("username"),
                        databaseProps.getProperty("password"));
                String query = "SELECT id, creation, valid, ST_AsText(fence) as geo_fence FROM " + databaseProps.getProperty("table");
                Statement statement = conn_db.createStatement();
                ResultSet resultSet = statement.executeQuery(query);
                return resultSetToCollection(resultSet);
            }
            catch (SQLException e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
                System.exit(-2);
            }
            return new ArrayList<>();
    }
}
