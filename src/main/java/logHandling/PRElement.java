package logHandling;

import java.util.List;

public class PRElement {
    public final long id;
    public List<Long> physicalNodes;
    public final String type;

    public PRElement(long id, String type) {
        this.id = id;
        this.type = type;
    }
    public String getTypeId(){
        return type+id;
    }

    public void setPhysicalNode(List<Long> nodes) {
        this.physicalNodes = nodes;
    }
}
