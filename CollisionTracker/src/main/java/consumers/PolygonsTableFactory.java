package consumers;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.sedona.flink.expressions.Constructors;
import org.locationtech.jts.io.ParseException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.flink.table.api.Expressions.*;

public class PolygonsTableFactory implements TableFactory<ResultSet, Polygon> {

    @Override
    public Table createTable(StreamTableEnvironment tableEnv, ResultSet resultSet, String[] colNames) {
        List<Row> data = resultSetToCollection(resultSet);
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

    @Override
    public Table createGeometryTable(String[] colNames, Table sourceTable) {
        Table validOnly = filterValidPolygons(sourceTable, colNames[2]);
        return validOnly.select(
                call(new Constructors.ST_GeomFromWKT(), $(colNames[1])).as(colNames[1]),
                $(colNames[0])
        );
    }

    @Override
    public Row createWKTGeometry(Polygon polygon) {
        Row row = Row.of(polygon.id, polygon.fence, polygon.valid, polygon.creation );
        return row;
    }

    /* Returns only polygons that are valid */
    private static Table filterValidPolygons(Table table, String valid){
        return table.filter($(valid).isTrue());
    }

    // Helper method to convert ResultSet from database to collection of class Polygon
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
            System.out.println(fence);
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
}