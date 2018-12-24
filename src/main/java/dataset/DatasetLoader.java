package dataset;

import cluster.PartitionMapper;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.Map;

public interface DatasetLoader {
    /**
     * Loads the dataset to the graph.
     * @param graph for the dataset to be loaded.
     * @param partitionMapper for mapping the vertex partition to the database.
     * @return Map of cluster partitions and their usage/capacity.
     * @throws IOException when reading the dataset files goes wrong.
     */
    Map<Long, Pair<Long, Long>> loadDatasetToGraph(StandardJanusGraph graph, PartitionMapper partitionMapper) throws IOException;

    /**
     * Helps fill the cluster map that is returned in tthe {@link DatasetLoader#loadDatasetToGraph(StandardJanusGraph, PartitionMapper)}
     * @param cluster the map to be filled.
     * @param partitionMapper for getting the partition of the vertexId
     * @param vertexId the id of a vertex
     */
    static void computeClusterHelper(Map<Long, Pair<Long, Long>> cluster, PartitionMapper partitionMapper, long vertexId) {
        cluster.compute(partitionMapper.map(vertexId), (k, v) -> {
            if (v == null) {
                return new Pair<>(20000000L, 1L);
            } else
                return new Pair<>(20000000L, v.getValue1() + 1);
        });
    }

    /**
     * Increment the cluster partition usage.
     * @param cluster the map to be filled.
     * @param PID the ID of the partition which usage should be incremented
     */
    static void computeClusterHelper(Map<Long, Pair<Long, Long>> cluster, long PID) {
        cluster.compute(PID, (k, v) -> {
            if (v == null) {
                return new Pair<>(20000000L, 1L);
            } else
                return new Pair<>(20000000L, v.getValue1() + 1);
        });
    }
}
