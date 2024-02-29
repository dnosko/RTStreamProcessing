package consumers;

import java.time.LocalDateTime;

public class PolygonEnterEvent extends PolygonOutputEvent{
    public String collision_point_in;
    public long collision_date_in;

    public PolygonEnterEvent(int polygon, int device, boolean in, String collision_point_in, long collision_date_in){
        super(polygon,device,in, eventTypeEnum.ENTER_EVENT);
        this.collision_point_in = collision_point_in;
        this.collision_date_in = collision_date_in;

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"event_type\": \"").append(eventType.getValue()).append("\", ");
        sb.append("\"polygon\": ").append(polygon).append(", ");
        sb.append("\"device\": ").append(device).append(", ");
        sb.append("\"in\": ").append(in).append(", ");
        sb.append("\"collision_point_in\": ").append(pointToFormattedString(collision_point_in)).append(", ");
        sb.append("\"collision_date_in\": \"").append(collision_date_in).append("\"");
        sb.append("}");
        return sb.toString();
    }
}
