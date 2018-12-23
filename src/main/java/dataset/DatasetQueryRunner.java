package dataset;

import cluster.PartitionMapper;
import logHandling.PRLogRecord;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.Iterator;

public interface DatasetQueryRunner {
    /**
     * Runs the queries "benchmark" queries against the dataset.
     * @param graph for the queries to be run against
     * @param partitionMapper
     * @param log true/false to enable/disable logging of the queries to the {@link logHandling.PRLog}
     */
    void runQueries(StandardJanusGraph graph, PartitionMapper partitionMapper, boolean log);

    /**
     * Evaluates the improvement and other metrics of the partitioning algorithm
     * @param result the computer result fo the partitioning algorithm
     * @param label
     * @return the improvement in percents - the difference of before and after cross node communication.
     * @throws Exception
     */
    double evaluateQueries(ComputerResult result, String label) throws Exception;

    /**
     * Variant of the {@link DatasetQueryRunner#evaluateQueries(ComputerResult, String)}
     */
    double evaluateQueries(Graph graph, String label) throws Exception;

    /**
     * Variant of the {@link DatasetQueryRunner#evaluateQueries(ComputerResult, String)}
     */
    double evaluateQueries(Graph graph, String label, Iterator<PRLogRecord> log) throws Exception;

}
