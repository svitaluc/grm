import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class PhysicalNodeDetector {
    public static List<List<Long>> getPhysicalNodesOfData(List<?> data) {
        Random r = new Random();
        List<List<Long>> res = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            res.add(r.longs(3,0,20).boxed().collect(Collectors.toList()));
        }
        return res;
    }

}
