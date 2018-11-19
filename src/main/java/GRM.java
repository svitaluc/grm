import helpers.ProcessedResultLogGraphFactory;
import logHandling.LogFileLoader;
import logHandling.MyLog;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.clustering.vacquero.VacqueroVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.cql.CQLStoreManager;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.javatuples.Pair;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.process.computer.clustering.vacquero.VacqueroVertexProgram.CLUSTERS;
import static org.apache.tinkerpop.gremlin.process.computer.clustering.vacquero.VacqueroVertexProgram.LABEL;

public class GRM {
    private PropertiesConfiguration config;
    private MyLog log;
    private StandardJanusGraph logGraph;
    private boolean isLogGraphSameAsRealGraph = false;
    private StandardJanusGraph realGraph;
    private CQLStoreManager realGraphSM;
    private GraphTraversalSource lg;
    private GraphTraversalSource realg;
    String graphLogPropFileName,graphRealPropFileName;
    private Map<Long, Pair<Long, Long>> clusters;
    public static final String PARTITION_LABEL = LABEL;
    public static final String EDGE_LABEL = ProcessedResultLogGraphFactory.EDGE_LABEL;
    public static final long CLUSTER_CAPACITY = 20000L; //TODO make this configurable
    private ComputerResult algorithmResult;
    private StaticVertexProgram<Pair<Serializable, Long>> vertexProgram;

    public GRM() throws ConfigurationException {
        this.config = new PropertiesConfiguration("config.properties");
    }

    public static void main(String[] args) throws Exception {
        GRM grm = new GRM();
        grm.loadLog("C:\\Users\\black\\OneDrive\\Dokumenty\\programLucka\\src\\main\\resources\\processedLog");
        grm.connectToLogGraph();
        grm.saveLogToGraph();
//        grm.connectToRealGraph();
//        grm.getPhysicalPartitions();
//        grm.runPartitioningAlgorithm();
//        grm.evaluateResults();
//        System.exit(0);
    }

    private void loadLog(String file) throws IOException {
        this.log = LogFileLoader.load(new File(file));
    }

    private void connectToLogGraph() throws ConfigurationException {
        Configuration conf = new PropertiesConfiguration(graphLogPropFileName);
        logGraph = (StandardJanusGraph) GraphFactory.open(conf);
    }

    private void saveLogToGraph() throws BackendException, ConfigurationException {
        //TODO handle updates and same logGraph as realGraph
        if(!isLogGraphSameAsRealGraph) {
//            logGraph.close();
            logGraph.getBackend().clearStorage();
            Configuration conf = new PropertiesConfiguration(graphLogPropFileName);
            logGraph = (StandardJanusGraph) GraphFactory.open(conf);
        }
        lg = logGraph.traversal();
        ProcessedResultLogGraphFactory.createSchemaQuery(logGraph);
        ProcessedResultLogGraphFactory.createElements(lg, log);
    }

    private void connectToRealGraph() throws ConfigurationException {
        Configuration conf = new PropertiesConfiguration(graphRealPropFileName);
        realGraph = (StandardJanusGraph) GraphFactory.open(conf);
        realGraphSM = (CQLStoreManager) realGraph.getBackend().getStoreManager();
        realg = logGraph.traversal();
    }

    private void getPhysicalPartitions() {
        IDManager idm = realGraph.getIDManager();
        long verticesCount = realg.V().count().next();
        Map<Long, Pair<Long, Long>> clusters = new HashMap<>();
        lg.V().forEachRemaining((v -> {
             Long pid = idm.getPartitionId((Long) v.id()); // -- virtual ID partitioning
            /*Long pid = realGraphSM.getCluster().getMetadata() //physical host partitioning
                    .getReplicas("janusgraph", idm.getKey((Long) v.id()).asByteBuffer())
                    .iterator().next().getHostId().getLeastSignificantBits();*/
            clusters.computeIfAbsent(pid, k -> new Pair<>(CLUSTER_CAPACITY, 1L));
            clusters.computeIfPresent(pid, (k, val) -> new Pair<>(val.getValue0(), val.getValue1() + 1));
            v.property(PARTITION_LABEL, pid);
        }));
    }

    private void runPartitioningAlgorithm() throws ExecutionException, InterruptedException {
        vertexProgram = VacqueroVertexProgram.build().clusters(clusters).acquireLabelProbability(0.5).create(logGraph);
        algorithmResult = logGraph.compute().program(vertexProgram).submit().get();
    }

    private void evaluateResults() {
        GraphTraversalSource rg = algorithmResult.graph().traversal();
        System.out.println(Arrays.toString(algorithmResult.memory().<Map<Long, Pair<Long, Long>>>get(CLUSTERS).entrySet().toArray()));
    }

}