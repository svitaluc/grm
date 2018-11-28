package helpers;

import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.graphdb.database.StandardJanusGraph;

public interface DatasetQueryRunner {
    void runQueries(StandardJanusGraph graph, ClusterMapper clusterMapper);
    double evaluateQueries(ComputerResult result, String label) throws Exception;
    double evaluateQueries(Graph graph, String label) throws Exception;
}
