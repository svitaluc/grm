package helpers;

import logHandling.LogRecord;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.Iterator;

public interface LogToGraphLoader {
    static final String EDGE_LABEL = "queriedTogether";
    static final String EDGE_PROPERTY = "times";

    boolean addSchema(StandardJanusGraph graph);
    void removeSchema(StandardJanusGraph graph);

    void loadLogToGraph(StandardJanusGraph graph, Iterator<LogRecord> logRecords);
}
