import cluster.PartitionMapper;
import dataset.DatasetLoader;
import dataset.DatasetQueryRunner;
import logHandling.LogFileLoader;
import logHandling.PRLogRecord;
import logHandling.PRLogToGraphLoader;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.javatuples.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The base of the GRM implementations.
 */
public abstract class GRM {
    protected PropertiesConfiguration config;
    protected Iterator<PRLogRecord> logRecordIterator;
    protected String graphPropFile, logFile;
    protected StandardJanusGraph graph;
    protected Map<Long, Pair<Long, Long>> clusters;
    public static final long CLUSTER_CAPACITY = 20000L;
    protected ComputerResult algorithmResult;
    protected StaticVertexProgram<Pair<Serializable, Long>> vertexProgram;

    /**
     * Calculates the degree distribution for the dataset.
     */
    protected void printVertexDegrees() {
        final Map<Long, Long> tin = new HashMap<>();
        final Map<Long, Long> tout = new HashMap<>();
        final Map<Long, Long> tboth = new HashMap<>();
        GraphTraversalSource g = graph.traversal();
        graph.traversal().V().forEachRemaining(vertex -> {
            MapHelper.incr(tboth, g.V(vertex.id()).bothE().count().next(), 1L);
            MapHelper.incr(tin, g.V(vertex.id()).inE().count().next(), 1L);
            MapHelper.incr(tout, g.V(vertex.id()).outE().count().next(), 1L);
        });
        System.out.println("IN degree|count");
        for (Map.Entry<Long, Long> e : tin.entrySet()) {
            System.out.printf("%d\t\t%d\n", e.getKey(), e.getValue());
        }
        System.out.println("OUT degree|count");
        for (Map.Entry<Long, Long> e : tout.entrySet()) {
            System.out.printf("%d\t\t%d\n", e.getKey(), e.getValue());
        }
        System.out.println("BOTH degree|count");
        for (Map.Entry<Long, Long> e : tboth.entrySet()) {
            System.out.printf("%d\t\t%d\n", e.getKey(), e.getValue());
        }
    }

    protected void loadLog(String file) throws IOException {
        this.logRecordIterator = LogFileLoader.loadIterator(Paths.get("").toAbsolutePath().resolve(Paths.get(file)).toFile());
    }

    protected void closeGraph() throws BackendException {
        this.graph.getBackend().close();
        this.graph.close();

    }


    protected void injectLogToGraph(PRLogToGraphLoader loader) {
        loader.addSchema(graph);
        loader.loadLogToGraph(graph, logRecordIterator);
    }


    protected void connectToGraph() throws ConfigurationException {
        Configuration conf = new PropertiesConfiguration(graphPropFile);
        graph = (StandardJanusGraph) GraphFactory.open(conf);
    }

    protected void loadDataset(DatasetLoader loader, PartitionMapper clusterMapper) throws IOException {
        clusters = loader.loadDatasetToGraph(graph, clusterMapper);
    }

    protected void runTestQueries(DatasetQueryRunner runner, PartitionMapper clusterMapper, boolean log) {
        runner.runQueries(graph, clusterMapper, log);
    }

    protected void clearGraph() throws BackendException, ConfigurationException {
        if (graph == null) connectToGraph();
        graph.getBackend().clearStorage();
        System.out.println("Cleared the graph");
    }
}
