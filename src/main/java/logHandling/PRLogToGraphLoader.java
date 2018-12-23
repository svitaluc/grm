package logHandling;

import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.Iterator;

public interface PRLogToGraphLoader {
    static final String EDGE_LABEL = "queriedTogether";
    static final String EDGE_PROPERTY = "times";

    /**
     * Adds the schema for the PRLog to the graph.
     * @param graph for the schema to be loaded
     * @return
     */
    boolean addSchema(StandardJanusGraph graph);

    /**
     * Removes the schema for the PRLog from the graph.
     * @param graph for the schema to be removed
     */
    void removeSchema(StandardJanusGraph graph);

    /**
     * Iterates over the logRecords and loads each PRLogRecord to the graph.
     * @param graph for the PRLog to be loaded
     * @param logRecords the PRLogRecords to be loaded
     */
    void loadLogToGraph(StandardJanusGraph graph, Iterator<PRLogRecord> logRecords);
}
