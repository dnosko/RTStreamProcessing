package consumers;

import java.time.LocalDateTime;

public class PolygonLeaveEvent extends PolygonOutputEvent{
    public String collision_point_out;
    public LocalDateTime collision_date_out;

    public PolygonLeaveEvent(int polygon, int device, boolean in, String collision_point_out, LocalDateTime collision_date_out){
        super(polygon,device,in);
        this.collision_point_out = collision_point_out;
        this.collision_date_out = collision_date_out;

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"polygon\": ").append(polygon).append(", ");
        sb.append("\"device\": ").append(device).append(", ");
        sb.append("\"in\": ").append(in).append(", ");
        sb.append("\"collision_point_out\": \"").append(collision_point_out).append("\", ");
        sb.append("\"collision_date_out\": \"").append(collision_date_out).append("\"");
        sb.append("}");
        return sb.toString();
    }
}
