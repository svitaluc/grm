package backend;

import org.janusgraph.graphdb.database.StandardJanusGraph;

public interface PartitionIDGetter {
    default long getPID(StandardJanusGraph graph, long vertexID){
        return graph.getIDManager().getPartitionId(vertexID);
    }
}
