package consumers;

import java.util.Date;

public abstract class PolygonOutputEvent {
    public int polygon;
    public  int device;
    public  boolean in;

    public PolygonOutputEvent(int polygon, int device, boolean in){
        this.polygon = polygon;
        this.device = device;
        this.in = in;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"polygon\": ").append(polygon).append(", ");
        sb.append("\"device\": ").append(device).append(", ");
        sb.append("\"in\": ").append(in);
        sb.append("}");
        return sb.toString();
    }
}


