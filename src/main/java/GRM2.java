import helpers.DatasetLoader;
import helpers.DatasetQueryRunner;
import helpers.ProcessedResultLogGraphFactory;
import helpers.TwitterDatasetLoaderQueryRunner;
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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.computer.clustering.vacquero.VacqueroVertexProgram.LABEL;

public class GRM2 {
    private PropertiesConfiguration config;
    private MyLog log;
    private String graphPropFile, logFile;
    private StandardJanusGraph graph;
    private Map<Long, Pair<Long, Long>> clusters;
    public static final String PARTITION_LABEL = LABEL;
    public static final String EDGE_LABEL = ProcessedResultLogGraphFactory.EDGE_LABEL;
    public static final long CLUSTER_CAPACITY = 20000L; //TODO make this configurable
    private ComputerResult algorithmResult;
    private StaticVertexProgram<Pair<Serializable, Long>> vertexProgram;

    public GRM2() throws ConfigurationException {
        this.config = new PropertiesConfiguration("config.properties");
        this.graphPropFile = config.getString("graph.propFile");
        this.logFile = config.getString("log.logFile","C:\\Users\\black\\OneDrive\\Dokumenty\\programLucka\\src\\main\\resources\\processedLog");
    }

    public void initialize() throws IOException, ConfigurationException {
        if( config.getBoolean("log.readLog",true))
        connectToGraph();
    }

    public static void main(String[] args) throws Exception {
        GRM2 grm = new GRM2();
        TwitterDatasetLoaderQueryRunner twitter = new TwitterDatasetLoaderQueryRunner("C:\\Users\\black\\OneDrive\\Dokumenty\\programLucka\\src\\main\\resources\\datasets\\twitter");
//        grm.clearGraph();
        grm.connectToGraph();
//        grm.loadDataset(twitter);
        grm.runTestQueries(twitter);

    }

    private void loadLog(String file) throws IOException {
        this.log = LogFileLoader.load(new File(file));
    }

    private void connectToGraph() throws ConfigurationException {
        Configuration conf = new PropertiesConfiguration(graphPropFile);
        graph = (StandardJanusGraph) GraphFactory.open(conf);
    }

    private void loadDataset(DatasetLoader loader) throws IOException {
        loader.loadDatasetToGraph(graph);
    }

    private void runTestQueries(DatasetQueryRunner runner)  {
        runner.runQueries(graph);
    }

    private void clearGraph() throws BackendException, ConfigurationException {
        if(graph==null) connectToGraph();
        graph.getBackend().clearStorage();
        System.out.println("Cleared the graph");
    }


}