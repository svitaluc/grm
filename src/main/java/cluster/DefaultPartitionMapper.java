package cluster;

import org.janusgraph.graphdb.idmanagement.IDManager;

public class DefaultPartitionMapper implements PartitionMapper {
    private int numClusters;
    private IDManager idManager;

    @Override
    public long map(long vertexId) {
        if(idManager == null)
            return Math.abs(Long.hashCode(vertexId) % numClusters);
        return Math.abs(Long.hashCode(idManager.fromVertexId(vertexId)) % numClusters);
//        return Math.abs(vertexId % numClusters);
    }

    public DefaultPartitionMapper(int numClusters) {
        this.numClusters = numClusters;
    }

    public DefaultPartitionMapper(int numClusters, IDManager idManager) {
        this.numClusters = numClusters;
        this.idManager = idManager;
    }
}
