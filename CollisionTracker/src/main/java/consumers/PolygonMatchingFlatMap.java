package consumers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static consumers.CollisionTracker.locationColNames;
import static consumers.CollisionTracker.polygonColNames;

// IN, OUT, KEY, WINDOW
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
        LocalDateTime timestamp = (LocalDateTime) row.getField(2);
        int polygonID = (Integer) row.getField(4);
        boolean contains = (Boolean) row.getField(5);

        Iterable<Integer> currentlyInPolygons = inPolygons.get();
        List<Integer> currentList = new ArrayList<>();

        // copy the old list to a new one
        for (Integer v : currentlyInPolygons) {
            currentList.add(v);
            //TODO pridava to asi duplicitne do zoznamu hodnoty
            System.out.println(deviceID + ":" + v);
        }


        Integer isAlreadyInPolygon = isInPolygon(currentlyInPolygons,polygonID );

        PolygonOutputEvent  outEvent = null;


        if (isAlreadyInPolygon != null && !contains){ // device was in polygon, isnt anymore
            currentList.remove(isAlreadyInPolygon);
            outEvent = new PolygonOutEvent(polygonID, deviceID, false, point, timestamp );
        }
        else if (isAlreadyInPolygon == null && contains) { // device wasnt in polygon, is in polygon now
            currentList.add(polygonID);
            outEvent = new PolygonInEvent(polygonID, deviceID, true, point, timestamp);
        }
        inPolygons.clear();
        inPolygons.addAll(currentList);

        if (outEvent != null)
            out.collect(outEvent);
    }

    private Integer isInPolygon(Iterable<Integer> elements, int polygonID) {
        for (int alreadyIn : elements) {
            if (polygonID == alreadyIn) {
                return polygonID;
            }
        }
        return null;
    }

}
