/** Daša Nosková - xnosko05
 *  VUT FIT 2024
 **/

package consumers;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class PolygonMatchingFlatMap extends RichFlatMapFunction<Row, PolygonOutputEvent > {
    private transient ListState<Integer> inPolygons; // polygons in which the device is currently
    //public abstract KeyedStateStore globalState();

    @Override
    public void open(Configuration config) {
        ListStateDescriptor<Integer> descriptor =
                new ListStateDescriptor<>("inPolygons", TypeInformation.of(Integer.class));
        inPolygons = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void flatMap(Row row, Collector<PolygonOutputEvent > out) throws Exception {

        int deviceID = (Integer) row.getField(1);
        String point = row.getField(0).toString();
        LocalDateTime timestampRow = (LocalDateTime) row.getField(2);
        Timestamp timestamp = Timestamp.valueOf(timestampRow);

        int polygonID = (Integer) row.getField(4);
        boolean contains = (Boolean) row.getField(5);

        Iterable<Integer> currentlyInPolygons = inPolygons.get();
        List<Integer> currentList = new ArrayList<>();

        // copy the old list to a new one
        for (Integer v : currentlyInPolygons) {
            currentList.add(v);
            //System.out.println(deviceID + ":" + v);
        }


        Integer isAlreadyInPolygon = isInPolygon(currentlyInPolygons,polygonID );

        PolygonOutputEvent  outEvent = null;


        if (isAlreadyInPolygon != null && !contains){ // device was in polygon, isnt anymore so device left polygon
            currentList.remove(isAlreadyInPolygon);
            outEvent = new PolygonExitEvent(polygonID, deviceID, false, point, timestamp.getTime());
        }
        else if (isAlreadyInPolygon == null && contains) { // device wasnt in polygon, is in polygon now so device entered polygon
            currentList.add(polygonID);
            outEvent = new PolygonEnterEvent(polygonID, deviceID, true, point, timestamp.getTime());
        }
        inPolygons.clear();
        inPolygons.addAll(currentList);

        if (outEvent != null) {
            out.collect(outEvent);
            //System.out.println(outEvent);
        }
    }

    private Integer isInPolygon(Iterable<Integer> elements, int polygonID) {
        for (int alreadyIn : elements) {
            if (polygonID == alreadyIn) {
                return alreadyIn;
            }
        }
        return null;
    }

}
