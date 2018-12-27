import cluster.DefaultPartitionMapper;
import cluster.PartitionMapper;
import dataset.DatasetQueryRunner;
import dataset.TwitterDatasetLoaderQueryRunner;
import logHandling.DefaultPRLogToGraphLoader;
import logHandling.PRLogToGraphLoader;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.javatuples.Pair;
import partitioningAlgorithms.VaqueroVertexProgram;

import java.awt.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static partitioningAlgorithms.VaqueroVertexProgram.CLUSTER;
import static partitioningAlgorithms.VaqueroVertexProgram.CLUSTER_LOWER_BOUND_SPACE;

/**
 * This class handles the lifecycle of running the "benchmark" on the Twitter EGO network dataset.
 */
public class GRMT extends GRM {

    public GRMT() throws ConfigurationException {
        this.config = new PropertiesConfiguration("config.properties");
        this.graphPropFile = config.getString("graph.propFile");
        this.logFile = config.getString("log.logFile", "./processedLog");
    }

    public void initialize() throws ConfigurationException {
        if (config.getBoolean("log.readLog", true))
            connectToGraph();
    }

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();
        System.out.println("Started: " + new SimpleDateFormat().format(new Date(time)));
        GRMT grm = new GRMT();
        TwitterDatasetLoaderQueryRunner twitter = new TwitterDatasetLoaderQueryRunner("src/main/resources/datasets/twitter");
        PRLogToGraphLoader logLoader = new DefaultPRLogToGraphLoader();
        PartitionMapper clusterMapper = new DefaultPartitionMapper(3);

        //dataset part
        grm.clearGraph();
        grm.connectToGraph();
        grm.loadDataset(twitter, clusterMapper);
//        grm.printVertexDegrees();
        grm.runTestQueries(twitter, clusterMapper, true); // not needed when the log is already created

        //log part
        grm.loadLog(grm.logFile);
        grm.injectLogToGraph(logLoader);
        for (int i = 0; i < 10; i++) {
            grm.runPartitioningAlgorithm(clusterMapper, twitter);
            grm.evaluatePartitioningAlgorithm(twitter);
        }
        Toolkit.getDefaultToolkit().beep();
        System.exit(0);
        //validation part
        System.out.println("Running validating evaluation");
        TwitterDatasetLoaderQueryRunner twitterValidate = new TwitterDatasetLoaderQueryRunner(2L, "src/main/resources/datasets/twitter");
        grm.runTestQueries(twitterValidate, clusterMapper, false);
        grm.evaluatePartitioningAlgorithm(twitterValidate);

        System.out.printf("Total runtime: %.2fs\n", (System.currentTimeMillis() - time) / 1000D);
        Toolkit.getDefaultToolkit().beep();

        grm.closeGraph();
        System.exit(0);
    }


    private void runPartitioningAlgorithm(PartitionMapper cm, TwitterDatasetLoaderQueryRunner runner) throws ExecutionException, InterruptedException {
        vertexProgram = VaqueroVertexProgram.build()
                .clusterMapper(cm)
                .acquirePartitionProbability(0.5)
                .imbalanceFactor(0.5)
                .coolingFactor(0.99)
                .adoptionFactor(1)
                .scopeIncidentTraversal(__::bothE)
                .evaluatingMap(runner.evaluatingMap())
                .evaluateCrossCommunication(true)
                .evaluatingStatsOriginal(runner.evaluatingStats())
                .maxPartitionChangeRatio(1)
                .maxIterations(200).create(graph);
        algorithmResult = graph.compute().program(vertexProgram).workers(24).submit().get();
        System.out.println("Clusters capacity/usage: " + Arrays.toString(algorithmResult.memory().<Map<Long, Pair<Long, Long>>>get(CLUSTER).entrySet().toArray()));
        System.out.println("Clusters Lower Bound: " + Arrays.toString(algorithmResult.memory().<Map<Long, Long>>get(CLUSTER_LOWER_BOUND_SPACE).entrySet().toArray()));
        System.out.println("Clusters added together count: " + algorithmResult.memory().<Map<Long, Pair<Long, Long>>>get(CLUSTER).values().stream().mapToLong(Pair::getValue1).reduce((left, right) -> left + right).getAsLong());
        System.out.println("Vertex count: " + graph.traversal().V().count().next());
        graph.tx().commit();
    }

    private void evaluatePartitioningAlgorithm(DatasetQueryRunner runner) throws Exception {
        runner.evaluateQueries(graph, VaqueroVertexProgram.PARTITION);
    }
}