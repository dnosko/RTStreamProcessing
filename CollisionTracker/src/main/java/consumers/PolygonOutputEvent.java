package consumers;

import java.util.Date;

public abstract class PolygonOutputEvent {

    public enum eventTypeEnum {
        EXIT_EVENT("exit"), ENTER_EVENT("enter");

        private final String value;

        eventTypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
    public int polygon;
    public  int device;
    public  boolean in;

    public eventTypeEnum eventType;

    public PolygonOutputEvent(int polygon, int device, boolean in, eventTypeEnum eventType){
        this.polygon = polygon;
        this.device = device;
        this.in = in;
        this.eventType = eventType;
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


