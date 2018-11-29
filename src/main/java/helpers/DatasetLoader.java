package helpers;

import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.Map;

public interface DatasetLoader {
    Map<Long, Pair<Long, Long>> loadDatasetToGraph(StandardJanusGraph graph, ClusterMapper clusterMapper) throws IOException;
}
