package helpers;

import logHandling.LogRecord;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.Iterator;

public interface DatasetQueryRunner {
    void runQueries(StandardJanusGraph graph, ClusterMapper clusterMapper, boolean log);
    double evaluateQueries(ComputerResult result, String label) throws Exception;
    double evaluateQueries(Graph graph, String label) throws Exception;
    double evaluateQueries(Graph graph, String label, Iterator<LogRecord> log) throws Exception;
}
