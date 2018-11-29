package helpers;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.LogPathStrategy;
import org.apache.tinkerpop.gremlin.structure.*;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.javatuples.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


public class TwitterDatasetLoaderQueryRunner implements DatasetLoader, DatasetQueryRunner {
    private Path datasetPath;
    private final Map<Long, Long> vertexIdsDegrees = new HashMap<>();
    private final Set<Long> vertexIdsExpanded = new HashSet<>();
    private final List<Long> tweetsReaders = new ArrayList<>();
    private final Map<Long,Long> allTweetsReaders = new HashMap<>();
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
        this.datasetPath = Paths.get(path);
    }

    public TwitterDatasetLoaderQueryRunner(String path) {
        this(RANDOM_SEED_ORIGINAl,path);
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
    public Map<Long, Pair<Long, Long>> loadDatasetToGraph(StandardJanusGraph graph, ClusterMapper clusterMapper) throws IOException {
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
//            if (i / filesToScan.size() > 0.8) break; //TODO remove this limit
            System.out.printf("%.2f%%\t%s\n", i / filesToScan.size() * 100, file.getName());
            Long id = iDmanager.toVertexId(Long.decode(file.getName().split("\\.")[0]));
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                JanusGraphVertex ego = (JanusGraphVertex) g.V(id).tryNext().orElse(null);
                if (ego == null) {
                    ego = graph.addVertex(T.label, VERTEX_LABEL, T.id, id);
                    computeClusterHelper(clusters, clusterMapper, id);
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
                        computeClusterHelper(clusters, clusterMapper, id1);
                    }

                    JanusGraphVertex b = (JanusGraphVertex) g.V(id2).tryNext().orElse(null);
                    if (b == null) {
                        b = graph.addVertex(T.label, VERTEX_LABEL, T.id, id2);
                        computeClusterHelper(clusters, clusterMapper, id2);
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

    public static void computeClusterHelper(Map<Long, Pair<Long, Long>> clusters, ClusterMapper clusterMapper, long id) {
        clusters.compute(clusterMapper.map(id), (k, v) -> {
            if (v == null) {
                return new Pair<>(20000000L, 1L);
            } else
                return new Pair<>(20000000L, v.getValue1() + 1);
        });
    }

    @Override
    public void runQueries(StandardJanusGraph graph, ClusterMapper clusterMapper, boolean log) {
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
        long queryLimit = vertexCount / 16;
        avgDegree /= vertexCount;

        System.out.printf("Vertex count: %d, Max degree: %d, Avg. degree: %.2f\n", vertexCount, maxDegree, avgDegree);
        List<Map.Entry<Long,Long>> vertexIdsDegreesList = new ArrayList<>(vertexIdsDegrees.entrySet());
        while (tweetsReaders.size() < queryLimit) {
            Map.Entry<Long, Long> pair = vertexIdsDegreesList.get(random.nextInt(Math.toIntExact(vertexCount)));
            if (pair.getValue() / 1D / vertexCount > random.nextDouble()) {
                tweetsReaders.add(pair.getKey()); //add the vertex id to the list provided the probability
            }
        }
        random = new Random(RANDOM_SEED);
        if (log)
        g = graph.traversal().withStrategies(LogPathStrategy.instance()); // enable the logging strategy
        else
            g = graph.traversal(); // disabled logging

        for (Long vid : tweetsReaders) {
            MapHelper.incr(allTweetsReaders,vid,1L);
            for (GraphTraversal<Vertex, Vertex> it = g.V(vid).out(EDGE_LABEL); it.hasNext(); ) {
                Vertex vertex = it.next();
                if (clusterMapper.map(vid) != clusterMapper.map((Long) vertex.id()))
                    originalCrossNodeQueries++;
                else
                    originalNodeQueries++;
                if (random.nextDouble() < avgDegree/(vertexIdsDegrees.get(vid)+avgDegree)) { //probability of expanding another twitterUser
                    MapHelper.incr(allTweetsReaders,(Long) vertex.id(),1L);
                    for (GraphTraversal<Vertex, Vertex> it1 = g.V(vertex.id()).out(EDGE_LABEL); it1.hasNext(); ) {
                        Vertex otherVertex = it1.next();
                        vertexIdsExpanded.add((Long) otherVertex.id());
                        if (clusterMapper.map((Long) otherVertex.id()) != clusterMapper.map((Long) vertex.id()))
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
        GraphTraversalSource g = graph.traversal();

        for (Map.Entry<Long,Long> entry : allTweetsReaders.entrySet()) {
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
}
