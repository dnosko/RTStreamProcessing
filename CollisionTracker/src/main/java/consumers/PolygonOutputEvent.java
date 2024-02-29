package consumers;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public String pointToFormattedString(String point){
        Pattern pattern = Pattern.compile("POINT \\((\\d+\\.\\d+) (\\d+\\.\\d+)\\)");
        Matcher matcher = pattern.matcher(point);
        if (matcher.find()) {
            double x = Double.parseDouble(matcher.group(1));
            double y = Double.parseDouble(matcher.group(2));
            return  "{\"x\": " + x +", \"y\": " + y +"}";
        }
        return null;
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


