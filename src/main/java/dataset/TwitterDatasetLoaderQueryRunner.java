package dataset;

import cluster.PartitionMapper;
import com.google.common.collect.Iterators;
import logHandling.PRLogRecord;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProcessedResultLoggingStrategy;
import org.apache.tinkerpop.gremlin.structure.*;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.javatuples.Pair;
import org.javatuples.Quartet;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiFunction;

import static logHandling.PRLogToGraphLoader.EDGE_PROPERTY;


public class TwitterDatasetLoaderQueryRunner implements DatasetLoader, DatasetQueryRunner {
    private Path datasetPath;
    private final Map<Long, Long> vertexIdsDegrees = new HashMap<>();
    private final Set<Long> vertexIdsExpanded = new HashSet<>();
    private final List<Long> tweetsReaders = new ArrayList<>();
    private final Map<Long, Long> allTweetsReaders = new HashMap<>();
    private long maxDegree = 0;
    private double avgDegree = 0;
    private long originalCrossNodeQueries = 0;
    private long originalNodeQueries = 0;
    private long repartitionedCrossNodeQueries = 0;
    private long repartitionedNodeQueries = 0;


    private static final long RANDOM_SEED_ORIGINAl = 123456L; //Original
    private final long RANDOM_SEED;
    private static final String VERTEX_LABEL = "TwitterUser";
    private static final String EDGE_LABEL = "follows";

    public TwitterDatasetLoaderQueryRunner(long seed, String path) {
        this.RANDOM_SEED = seed;
        this.datasetPath = Paths.get("").toAbsolutePath().resolve(Paths.get(path));
    }

    public TwitterDatasetLoaderQueryRunner(String path) {
        this(RANDOM_SEED_ORIGINAl, path);
    }

    private boolean createSchemaQuery(StandardJanusGraph graph) {
        JanusGraphManagement management = graph.openManagement();
        boolean created;
        if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
            management.rollback();
            created = false;
        } else {
            management.makeVertexLabel(VERTEX_LABEL).make();

            management.makeEdgeLabel(EDGE_LABEL).directed().multiplicity(Multiplicity.SIMPLE).make();
            management.buildIndex("followsIndex", Edge.class);
            management.buildIndex("vIndex", Vertex.class);
//            management.buildIndex("id",Vertex.class).addKey(management.getPropertyKey("id"));
            management.commit();
            created = true;
        }
        System.out.println(created ? "Successfully created the graph schema" : "The schema was already created");
        return created;
    }


    @Override
    public Map<Long, Pair<Long, Long>> loadDatasetToGraph(StandardJanusGraph graph, PartitionMapper partitionMapper) throws IOException {
        Map<Long, Pair<Long, Long>> clusters = new HashMap<>();
        createSchemaQuery(graph);
        GraphTraversalSource g = graph.traversal();
        g.tx().open();
        IDManager iDmanager = graph.getIDManager();

        List<File> filesToScan = new ArrayList<>();
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(datasetPath)) {
            for (Path file : directoryStream) {
                if (file.toString().endsWith(".edges"))
                    filesToScan.add(file.toFile());
            }
        }
        double i = 0;
        for (File file : filesToScan) {
            i++;
//            if(i > 10) break;
            System.out.printf("%.2f%%\t%s\n", i / filesToScan.size() * 100, file.getName());
            Long id = iDmanager.toVertexId(Long.decode(file.getName().split("\\.")[0]));
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                JanusGraphVertex ego = (JanusGraphVertex) g.V(id).tryNext().orElse(null);
                if (ego == null) {
                    ego = graph.addVertex(T.label, VERTEX_LABEL, T.id, id);
                    DatasetLoader.computeClusterHelper(clusters, partitionMapper, id);
                }

                String line;
                while ((line = reader.readLine()) != null) {
                    String[] splits = line.split("\\s+");
                    if (splits[0].equals(splits[1])) continue; //we don't allow self following
                    Long id1 = iDmanager.toVertexId(Long.decode(splits[0]));
                    Long id2 = iDmanager.toVertexId(Long.decode(splits[1]));

                    JanusGraphVertex a = (JanusGraphVertex) g.V(id1).tryNext().orElse(null);
                    if (a == null) {
                        a = graph.addVertex(T.label, VERTEX_LABEL, T.id, id1);
                        DatasetLoader.computeClusterHelper(clusters, partitionMapper, id1);
                    }

                    JanusGraphVertex b = (JanusGraphVertex) g.V(id2).tryNext().orElse(null);
                    if (b == null) {
                        b = graph.addVertex(T.label, VERTEX_LABEL, T.id, id2);
                        DatasetLoader.computeClusterHelper(clusters, partitionMapper, id2);
                    }

//                    System.out.println("\t\t" + id1 + "\t" + id2);

                    try {
                        a.addEdge(EDGE_LABEL, b);
                    } catch (SchemaViolationException ignored) {
                    }
                    try {
                        a.addEdge(EDGE_LABEL, ego);
                    } catch (SchemaViolationException ignored) {
                    }
                    try {
                        b.addEdge(EDGE_LABEL, ego);
                    } catch (SchemaViolationException ignored) {
                    }
                }
            }
        }
        g.tx().commit();
        System.out.println("Clusters populated to vertex count of: " + clusters.values().stream().mapToLong(Pair::getValue1).reduce(0, (left, right) -> left + right));
        System.out.println("Clusters: " + Arrays.toString(clusters.entrySet().toArray()));
        return clusters;
    }



    @Override
    public void runQueries(StandardJanusGraph graph, PartitionMapper partitionMapper, boolean log) {
        originalCrossNodeQueries = originalNodeQueries = repartitionedCrossNodeQueries = repartitionedNodeQueries = 0;
        System.out.println("Running test queries");
        Random random = new Random(RANDOM_SEED);
        GraphTraversalSource g = graph.traversal();
//        for (GraphTraversal<Vertex, Vertex> it = g.V().limit(50); it.hasNext(); ) { //testing limit
        for (GraphTraversal<Vertex, Vertex> it = g.V(); it.hasNext(); ) {
            Vertex vertex = it.next();
            long degree = Iterators.size(vertex.edges(Direction.OUT));
            avgDegree += degree;
            vertexIdsDegrees.put((Long) vertex.id(), degree);
            if (degree > maxDegree) maxDegree = degree;
        }
        long vertexCount = vertexIdsDegrees.size();
//        long queryLimit = 50; // testing limit
        long queryLimit = vertexCount / 8;
        long secondExpandCount = 0;
        avgDegree /= vertexCount;

        System.out.printf("Vertex count: %d, Max degree: %d, Avg. degree: %.2f\n", vertexCount, maxDegree, avgDegree);
        List<Map.Entry<Long, Long>> vertexIdsDegreesList = new ArrayList<>(vertexIdsDegrees.entrySet());
        while (tweetsReaders.size() < queryLimit) {
            Map.Entry<Long, Long> pair = vertexIdsDegreesList.get(random.nextInt(Math.toIntExact(vertexCount)));
            if (Math.log(Math.max(2, pair.getValue())) / Math.log(maxDegree) > random.nextDouble()) {
                tweetsReaders.add(pair.getKey()); //add the vertex id to the list provided the probability
            }
        }
        random = new Random(RANDOM_SEED);
        if (log)
            g = graph.traversal().withStrategies(ProcessedResultLoggingStrategy.instance()); // enable the logging strategy
        else
            g = graph.traversal(); // disabled logging

        for (Long vid : tweetsReaders) {
            MapHelper.incr(allTweetsReaders, vid, 1L);
            for (GraphTraversal<Vertex, Vertex> it = g.V(vid).out(EDGE_LABEL); it.hasNext(); ) {
                Vertex vertex = it.next();
                if (partitionMapper.map(vid) != partitionMapper.map((Long) vertex.id()))
                    originalCrossNodeQueries++;
                else
                    originalNodeQueries++;
                if (secondExpandCount < queryLimit && random.nextDouble() < avgDegree / (vertexIdsDegrees.get(vid) + avgDegree)) {//probability of expanding another twitterUser
                    secondExpandCount++;
                    MapHelper.incr(allTweetsReaders, (Long) vertex.id(), 1L);
                    for (GraphTraversal<Vertex, Vertex> it1 = g.V(vertex.id()).out(EDGE_LABEL); it1.hasNext(); ) {
                        Vertex otherVertex = it1.next();
                        vertexIdsExpanded.add((Long) otherVertex.id());
                        if (partitionMapper.map((Long) otherVertex.id()) != partitionMapper.map((Long) vertex.id()))
                            originalCrossNodeQueries++;
                        else
                            originalNodeQueries++;

                    }
                }
            }
        }


    }

    public Map<Long, Long> evaluatingMap() {
        return allTweetsReaders;
    }

    public Pair<Long, Long> evaluatingStats() {
        return new Pair<>(originalNodeQueries, originalCrossNodeQueries);
    }

    @Override
    public double evaluateQueries(ComputerResult result, String label) throws Exception {
        return this.evaluateQueries(result.graph(), label);
    }

    @Override
    public double evaluateQueries(Graph graph, String label) throws Exception {
        if (tweetsReaders.size() == 0 || vertexIdsDegrees.size() == 0)
            throw new Exception("The dataset runner must run the queries first before the result evaluation");
        repartitionedCrossNodeQueries = repartitionedNodeQueries = 0;
        GraphTraversalSource g = graph.traversal();
        System.out.println("Number of all to be expanded nodes: " + allTweetsReaders.size());
        for (Map.Entry<Long, Long> entry : allTweetsReaders.entrySet()) {
            long vid = entry.getKey();
            long times = entry.getValue();
            for (int i = 0; i < times; i++) {
                for (GraphTraversal<Vertex, Vertex> it = g.V(vid).out(EDGE_LABEL); it.hasNext(); ) {
                    Vertex vertex = it.next();
                    if (!g.V(vid).next().value(label).equals(vertex.value(label)))
                        repartitionedCrossNodeQueries++;
                    else
                        repartitionedNodeQueries++;
                }
            }

        }
        double improvement = (originalCrossNodeQueries - repartitionedCrossNodeQueries) / (double) originalCrossNodeQueries;
        System.out.printf("Before/After Cross Node Queries: %d / %d, Improvement: %.2f%%" +
                        "\nGood before/after Queries:  %d / %d\n"
                , originalCrossNodeQueries
                , repartitionedCrossNodeQueries
                , improvement * 100
                , originalNodeQueries
                , repartitionedNodeQueries
        );
        return improvement;
    }

    public static BiFunction<Quartet<Serializable, Long, Long, Long>, Edge, Quartet<Serializable, Long, Long, Long>> edgeFunction = (m, edge) -> {
        try {
            return m.setAt2(edge.<Long>value(EDGE_PROPERTY)).setAt3(-1L);
        } catch (Exception e) {
            return m.setAt2(-1L).setAt3(edge.vertices(Direction.OUT).hasNext() ? (Long) edge.vertices(Direction.OUT).next().id() : -1);
        }
    };

    @Override
    public double evaluateQueries(Graph graph, String label, Iterator<PRLogRecord> log) throws Exception {
        return 0;
    }
}
