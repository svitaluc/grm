package helpers;

import org.janusgraph.graphdb.database.StandardJanusGraph;

public interface DatasetQueryRunner {
    void runQueries(StandardJanusGraph graph);
}
