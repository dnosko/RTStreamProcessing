package consumers;

import java.time.LocalDateTime;
import java.util.Date;

public class PolygonInEvent extends PolygonOutputEvent{
    public String collision_point_in;
    public LocalDateTime collision_date_in;

    public PolygonInEvent(int polygon, int device, boolean in, String collision_point_in, LocalDateTime collision_date_in){
        super(polygon,device,in);
        this.collision_point_in = collision_point_in;
        this.collision_date_in = collision_date_in;

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"polygon\": ").append(polygon).append(", ");
        sb.append("\"device\": ").append(device).append(", ");
        sb.append("\"in\": ").append(in).append(", ");
        sb.append("\"collision_point_in\": \"").append(collision_point_in).append("\", ");
        sb.append("\"collision_date_in\": \"").append(collision_date_in).append("\"");
        sb.append("}");
        return sb.toString();
    }
}