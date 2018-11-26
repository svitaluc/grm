package helpers;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.janusgraph.graphdb.database.StandardJanusGraph;

public interface DatasetQueryRunner {
    void runQueries(StandardJanusGraph graph, ClusterMapper clusterMapper);
    void evaluateQueries(ComputerResult result, String label) throws Exception;
}
