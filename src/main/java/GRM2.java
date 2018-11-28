import helpers.*;
import logHandling.LogFileLoader;
import logHandling.MyLog;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.javatuples.Pair;
import partitioningAlgorithms.VaqueroVertexProgram;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static partitioningAlgorithms.VaqueroVertexProgram.CLUSTERS;
import static partitioningAlgorithms.VaqueroVertexProgram.CLUSTER_LOWER_BOUND_SPACE;

public class GRM2 {
    private PropertiesConfiguration config;
    private MyLog log;
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

    public void initialize() throws IOException, ConfigurationException {
        if (config.getBoolean("log.readLog", true))
            connectToGraph();
    }

    public static void main(String[] args) throws Exception {
        GRM2 grm = new GRM2();
        TwitterDatasetLoaderQueryRunner twitter = new TwitterDatasetLoaderQueryRunner("C:\\Users\\black\\OneDrive\\Dokumenty\\programLucka\\src\\main\\resources\\datasets\\twitter");
        LogToGraphLoader logLoader = new DefaultLogToGraphLoader();
        ClusterMapper clusterMapper = new ClusterMapper() {
        };

//        grm.clearGraph();
        grm.connectToGraph();
//        grm.loadDataset(twitter, clusterMapper);
        grm.runTestQueries(twitter, clusterMapper,false);
//        grm.loadLog(grm.logFile);
//        logLoader.removeSchema(grm.graph); //TODO remove schema has some issues while iterating the traversals later
//        grm.connectToGraph();
//        grm.injectLogToGraph(logLoader);
//        grm.runPartitioningAlgorithm(clusterMapper, twitter);
        grm.evaluatePartitioningAlgorithm(twitter);
        grm.closeGraph();
        System.exit(0);
    }

    private void closeGraph() throws BackendException {
        this.graph.getBackend().close();
        this.graph.close();

    }

    private void loadLog(String file) throws IOException {
        this.log = LogFileLoader.load(new File(file));

    }

    private void injectLogToGraph(LogToGraphLoader loader) throws IOException {
        loader.addSchema(graph);
        loader.loadLogToGraph(graph, log);
    }


    private void connectToGraph() throws ConfigurationException {
        Configuration conf = new PropertiesConfiguration(graphPropFile);
        graph = (StandardJanusGraph) GraphFactory.open(conf);
    }

    private void loadDataset(DatasetLoader loader, ClusterMapper clusterMapper) throws IOException {
        clusters = loader.loadDatasetToGraph(graph, clusterMapper);
    }

    private void runTestQueries(DatasetQueryRunner runner, ClusterMapper clusterMapper,boolean log) {
        runner.runQueries(graph, clusterMapper,log);
    }

    private void clearGraph() throws BackendException, ConfigurationException {
        if (graph == null) connectToGraph();
        graph.getBackend().clearStorage();
        System.out.println("Cleared the graph");
    }

    private void runPartitioningAlgorithm(ClusterMapper cm, TwitterDatasetLoaderQueryRunner runner) throws ExecutionException, InterruptedException {
        vertexProgram = VaqueroVertexProgram.build().clusters(clusters).clusterMapper(cm).acquireLabelProbability(0.5).evaluatingMap(runner.evaluatingMap()).evaluatingStatsOriginal(runner.evaluatingStats()).maxIterations(200).create(graph);
        algorithmResult = graph.compute().program(vertexProgram).workers(12).submit().get();
        System.out.println("Partition result: " + algorithmResult.graph().traversal().V().valueMap().next());
//        algorithmResult.graph().traversal().V().limit(20).forEachRemaining(vertex -> vertex.properties().forEachRemaining(objectVertexProperty -> System.out.println((vertex.id() + ": O-" + cm.map((Long) vertex.id()) + ": N-" + objectVertexProperty.value()))));
        System.out.println("Clusters capacity/usage: " + Arrays.toString(algorithmResult.memory().<Map<Long, Pair<Long, Long>>>get(CLUSTERS).entrySet().toArray()));
        System.out.println("Clusters Lower Bound: " + Arrays.toString(algorithmResult.memory().<Map<Long, Long>>get(CLUSTER_LOWER_BOUND_SPACE).entrySet().toArray()));
        System.out.println("Clusters added together count: " + algorithmResult.memory().<Map<Long, Pair<Long, Long>>>get(CLUSTERS).values().stream().mapToLong(Pair::getValue1).reduce((left, right) -> left + right).getAsLong());
        System.out.println("Vertex count: " + graph.traversal().V().count().next());
        graph.tx().commit();
    }

    private void evaluatePartitioningAlgorithm(DatasetQueryRunner runner) throws Exception {
        runner.evaluateQueries(graph, VaqueroVertexProgram.LABEL);
    }


}