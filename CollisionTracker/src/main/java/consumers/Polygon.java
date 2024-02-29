package consumers;

import lombok.Getter;

import java.util.Date;

public class Polygon {
    @Getter
    int id;
    @Getter
    String fence ;
    @Getter
    boolean valid;
    @Getter
    Date creation;

    public Polygon(int id, String fence, boolean valid, Date creation) {
        this.id = id;
        this.fence = fence;
        this.valid = valid;
        this.creation = creation;
    }

}
