import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.javatuples.Pair;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class GRM {
    private MyLog log;
    private Graph logGraph;
    private StandardJanusGraph realGraph;
    private GraphTraversalSource lg;
    private GraphTraversalSource realg;
    private String graphLogPropFileName;
    private String graphRealPropFileName;
    public static final String PARTITION_LABEL = "PID";
    private ComputerResult algorithmResult;
    private StaticVertexProgram<Pair<Serializable, Long>> vertexProgram;

    public static void main(String[] args) throws Exception {
        GRM grm = new GRM();
        grm.loadLog("log");
        grm.connectToLogGraph();
        grm.saveLogToGraph();
        grm.connectToRealGraph();
        grm.getPhysicalPartitions();
        grm.runPartitioningAlgorithm();
        grm.evaluateResults();
        System.exit(0);
    }

    private void loadLog(String file) throws IOException {
        this.log = LogFileLoader.load(new File(file));
    }

    private void connectToLogGraph() throws ConfigurationException {
        Configuration conf = new PropertiesConfiguration(graphLogPropFileName);
        logGraph = GraphFactory.open(conf);
        lg = logGraph.traversal();
    }

    private void saveLogToGraph() {
        //TODO handle updates and same logGraph as realGraph
        ProcessedResultLogGraphFactory.createElements(lg, log);
    }

    private void connectToRealGraph() throws ConfigurationException {
        Configuration conf = new PropertiesConfiguration(graphRealPropFileName);
        realGraph = (StandardJanusGraph) GraphFactory.open(conf);
        realg = logGraph.traversal();
    }

    private void getPhysicalPartitions() {
        IDManager idm = realGraph.getIDManager();
        lg.V().forEachRemaining((vertex -> vertex.property(PARTITION_LABEL, idm.getPartitionId((Long) vertex.id()))));
    }

    private void runPartitioningAlgorithm() throws ExecutionException, InterruptedException {
        algorithmResult = realGraph.compute().program(vertexProgram).submit().get(); //todo configure the program
    }

    private void evaluateResults() {
        GraphTraversalSource rg = algorithmResult.graph().traversal();
    }

}