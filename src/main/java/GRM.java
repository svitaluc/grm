import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.File;
import java.io.IOException;

public class GRM {
    private MyLog log;
    private Graph logGraph;
    private StandardJanusGraph realGraph;
    private GraphTraversalSource lg;
    private String graphLogPropFileName;
    public static void main(String[] args) throws IOException {

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

    private void saveLogToGraph(){
        //TODO handle updates and same logGraph as realGraph
        ProcessedResultLogGraphFactory.createElements(lg, log);
    }
    private void getPhysicalPartitions(){}
    private void runPartitioningAlgorithm(){}
    private void evaluateResults(){}

}