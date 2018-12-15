import com.google.common.collect.Iterators;
import helpers.*;
import logHandling.LogFileLoader;
import logHandling.LogRecord;
import logHandling.MyLog;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.javatuples.Pair;
import partitioningAlgorithms.VaqueroVertexProgram;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static partitioningAlgorithms.VaqueroVertexProgram.CLUSTERS;
import static partitioningAlgorithms.VaqueroVertexProgram.CLUSTER_LOWER_BOUND_SPACE;

public class GRM2 {
    private PropertiesConfiguration config;
    private Iterator<LogRecord> logRecordIterator;
    private String graphPropFile, logFile;
    private StandardJanusGraph graph;
    private Map<Long, Pair<Long, Long>> clusters;
    public static final long CLUSTER_CAPACITY = 20000L; //TODO make this configurable
    private ComputerResult algorithmResult;
    private StaticVertexProgram<Pair<Serializable, Long>> vertexProgram;

    public GRM2() throws ConfigurationException {
        this.config = new PropertiesConfiguration("config.properties");
        this.graphPropFile = config.getString("graph.propFile");
        this.logFile = config.getString("log.logFile", "C:\\Users\\black\\OneDrive\\Dokumenty\\programLucka\\processedLog");
    }

    public void initialize() throws ConfigurationException {
        if (config.getBoolean("log.readLog", true))
            connectToGraph();
    }

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();
        System.out.println("Started: " + new SimpleDateFormat().format(new Date(time)));
        GRM2 grm = new GRM2();
        TwitterDatasetLoaderQueryRunner twitter = new TwitterDatasetLoaderQueryRunner("C:\\Users\\black\\OneDrive\\Dokumenty\\programLucka\\src\\main\\resources\\datasets\\twitter");
        LogToGraphLoader logLoader = new DefaultLogToGraphLoader();
        ClusterMapper clusterMapper = new DefaultClusterMapper(3);

        //dataset part
        grm.clearGraph();
        grm.connectToGraph();
        grm.loadDataset(twitter, clusterMapper);
//        grm.printVertexDegrees();
        grm.runTestQueries(twitter, clusterMapper, true); // not needed when the log is already created

        //log part
        grm.loadLog(grm.logFile);
        grm.injectLogToGraph(logLoader);
        grm.runPartitioningAlgorithm(clusterMapper, twitter);
        grm.evaluatePartitioningAlgorithm(twitter);

        //validation part
        System.out.println("Running validating evaluation");
        TwitterDatasetLoaderQueryRunner twitterValidate = new TwitterDatasetLoaderQueryRunner(2L, "C:\\Users\\black\\OneDrive\\Dokumenty\\programLucka\\src\\main\\resources\\datasets\\twitter");
        grm.runTestQueries(twitterValidate, clusterMapper, false);
        grm.evaluatePartitioningAlgorithm(twitterValidate);

        System.out.printf("Total runtime: %.2fs\n", (System.currentTimeMillis() - time) / 1000D);
        Toolkit.getDefaultToolkit().beep();

        grm.closeGraph();
        System.exit(0);
    }

    private void closeGraph() throws BackendException {
        this.graph.getBackend().close();
        this.graph.close();

    }

    private void loadLog(String file) throws IOException {
        this.logRecordIterator = LogFileLoader.loadIterator(new File(file));

    }

    private void injectLogToGraph(LogToGraphLoader loader) {
        loader.addSchema(graph);
        loader.loadLogToGraph(graph, logRecordIterator);
    }


    private void connectToGraph() throws ConfigurationException {
        Configuration conf = new PropertiesConfiguration(graphPropFile);
        graph = (StandardJanusGraph) GraphFactory.open(conf);
    }

    private void loadDataset(DatasetLoader loader, ClusterMapper clusterMapper) throws IOException {
        clusters = loader.loadDatasetToGraph(graph, clusterMapper);
    }

    private void runTestQueries(DatasetQueryRunner runner, ClusterMapper clusterMapper, boolean log) {
        runner.runQueries(graph, clusterMapper, log);
    }

    private void clearGraph() throws BackendException, ConfigurationException {
        if (graph == null) connectToGraph();
        graph.getBackend().clearStorage();
        System.out.println("Cleared the graph");
    }

    private void runPartitioningAlgorithm(ClusterMapper cm, TwitterDatasetLoaderQueryRunner runner) throws ExecutionException, InterruptedException {
        vertexProgram = VaqueroVertexProgram.build()
                .clusterMapper(cm)
                .acquireLabelProbability(0.5)
                .imbalanceFactor(0.9)
                .coolingFactor(0.97)
                .evaluatingMap(runner.evaluatingMap())
                .evaluatingStatsOriginal(runner.evaluatingStats())
                .maxIterations(200).create(graph);
        algorithmResult = graph.compute().program(vertexProgram).workers(12).submit().get();
        System.out.println("Clusters capacity/usage: " + Arrays.toString(algorithmResult.memory().<Map<Long, Pair<Long, Long>>>get(CLUSTERS).entrySet().toArray()));
        System.out.println("Clusters Lower Bound: " + Arrays.toString(algorithmResult.memory().<Map<Long, Long>>get(CLUSTER_LOWER_BOUND_SPACE).entrySet().toArray()));
        System.out.println("Clusters added together count: " + algorithmResult.memory().<Map<Long, Pair<Long, Long>>>get(CLUSTERS).values().stream().mapToLong(Pair::getValue1).reduce((left, right) -> left + right).getAsLong());
        System.out.println("Vertex count: " + graph.traversal().V().count().next());
        graph.tx().commit();
    }

    private void evaluatePartitioningAlgorithm(DatasetQueryRunner runner) throws Exception {
        runner.evaluateQueries(graph, VaqueroVertexProgram.LABEL);
    }

    private void printVertexDegrees() {
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


}