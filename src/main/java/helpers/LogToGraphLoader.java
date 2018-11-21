package helpers;

import logHandling.MyLog;
import org.janusgraph.graphdb.database.StandardJanusGraph;

public interface LogToGraphLoader {
    static final String EDGE_LABEL = "queriedTogether";

    boolean addSchema(StandardJanusGraph graph);
    void removeSchema(StandardJanusGraph graph);

    void loadLogToGraph(StandardJanusGraph graph, MyLog log);
}
