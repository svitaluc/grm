import java.util.List;

public class MyElement {
    public final long id;
    public List<Long> physicalNodes;
    public final String type;

    public MyElement(long id, String type) {
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
