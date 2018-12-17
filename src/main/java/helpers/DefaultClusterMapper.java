package helpers;

import org.janusgraph.graphdb.idmanagement.IDManager;

public class DefaultClusterMapper implements ClusterMapper {
    private int numClusters;
    private IDManager idManager;

    @Override
    public long map(long vertexId) {
        if(idManager == null)
            return Math.abs(Long.hashCode(vertexId) % numClusters);
        return Math.abs(Long.hashCode(idManager.fromVertexId(vertexId)) % numClusters);
//        return Math.abs(vertexId % numClusters);
    }

    public DefaultClusterMapper(int numClusters) {
        this.numClusters = numClusters;
    }

    public DefaultClusterMapper(int numClusters, IDManager idManager) {
        this.numClusters = numClusters;
        this.idManager = idManager;
    }
}
