package helpers;

import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.IOException;

public interface DatasetLoader {
    enum labels{
        monitoredVertex,
        relation,
        queriedTogether
    }
    void loadDatasetToGraph(StandardJanusGraph graph) throws IOException;
}
