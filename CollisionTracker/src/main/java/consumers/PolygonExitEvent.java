package consumers;

import java.time.LocalDateTime;

public class PolygonExitEvent extends PolygonOutputEvent{
    public String collision_point_out;
    public long collision_date_out;

    public PolygonExitEvent(int polygon, int device, boolean in, String collision_point_out, long collision_date_out){
        super(polygon,device,in, eventTypeEnum.EXIT_EVENT);
        this.collision_point_out = collision_point_out;
        this.collision_date_out = collision_date_out;

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("\"event_type\": \"").append(eventType.getValue()).append("\", ");
            sb.append("\"polygon\": ").append(polygon).append(", ");
            sb.append("\"device\": ").append(device).append(", ");
            sb.append("\"in\": ").append(in).append(", ");
            sb.append("\"collision_point_out\":").append(pointToFormattedString(collision_point_out)).append(", ");
            sb.append("\"collision_date_out\": \"").append(collision_date_out).append("\"");
            sb.append("}");
        return sb.toString();
    }
}
