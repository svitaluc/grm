package helpers;

public class DefaultClusterMapper implements ClusterMapper {
    private int numClusters;

    @Override
    public long map(long vertexId) {
        return vertexId % numClusters;
    }

    public DefaultClusterMapper(int numClusters) {
        this.numClusters = numClusters;
    }
}
