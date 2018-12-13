package helpers;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProcessedResultLoggingStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.javatuples.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class PenssylvaniaDatasetLoaderQueryRunner implements DatasetLoader, DatasetQueryRunner {
    private Path datasetPath;
    private final Map<Long, Long> vertexIdsDegrees = Collections.synchronizedMap(new HashMap<>());
    private final List<Long> citiesToQuery = Collections.synchronizedList(new ArrayList<>());
    private final Set<Long> setCitiesToQuery = new HashSet<>();
    private final Map<Long, Long> allExpandedVertices = Collections.synchronizedMap(new HashMap<>());
    private AtomicLong maxDegree = new AtomicLong(0);
    private AtomicDouble avgDegree = new AtomicDouble(0);
    private long originalCrossNodeQueries = 0;
    private long originalNodeQueries = 0;
    private long repartitionedCrossNodeQueries = 0;
    private long repartitionedNodeQueries = 0;
    private static final long datasetLines = 3083800;
    private static final int randomWalkDistanceLimit = 10;
    private static final int randomWalkDistanceLow = 5;
    private Map<Pair<Long, Long>, Long> queryData = Collections.synchronizedMap(new HashMap<>());
    private static final long diameter = 786;


    private static final long RANDOM_SEED_ORIGINAl = 123456L; //Original
    private final long RANDOM_SEED;
    private static final String VERTEX_LABEL = "city";
    private static final String EDGE_LABEL = "roadTo";

    public PenssylvaniaDatasetLoaderQueryRunner(long seed, String path) {
        this.RANDOM_SEED = seed;
        this.datasetPath = Paths.get(path);
    }

    public PenssylvaniaDatasetLoaderQueryRunner(String path) {
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
            management.buildIndex("roadToIndex", Edge.class);
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
        Map<Long, Pair<Long, Long>> clusters = Collections.synchronizedMap(new HashMap<>());
        createSchemaQuery(graph);

        IDManager iDmanager = graph.getIDManager();
        System.out.println("Loading dataset to DB");
        AtomicLong edgeCount = new AtomicLong();
        try (BufferedReader reader = new BufferedReader(new FileReader(datasetPath.toFile()))) {
            AtomicLong i = new AtomicLong();
            final JanusGraphTransaction threadedTx = graph.newTransaction();
            reader.lines().parallel().forEach(s -> {
                i.getAndIncrement();
                if (s.startsWith("#")) return;
                String[] splits = s.split("\\s+");
                if (splits[0].equals(splits[1])) return; //we don't allow self cycles
                Long id1 = iDmanager.toVertexId(1 + Long.decode(splits[0]));
                Long id2 = iDmanager.toVertexId(1 + Long.decode(splits[1]));
                JanusGraphVertex a, b;
                a = (JanusGraphVertex) threadedTx.traversal().V(id1).tryNext().orElse(null);
                if (a == null) {
                    try {
                        a = threadedTx.addVertex(T.label, VERTEX_LABEL, T.id, id1);
                        computeClusterHelper(clusters, clusterMapper, id1);
                    } catch (IllegalArgumentException ex) {
                        a = (JanusGraphVertex) threadedTx.traversal().V(id1).next();
                    }
                }
                b = (JanusGraphVertex) threadedTx.traversal().V(id2).tryNext().orElse(null);
                if (b == null) {
                    try {
                        b = threadedTx.addVertex(T.label, VERTEX_LABEL, T.id, id2);
                        computeClusterHelper(clusters, clusterMapper, id2);
                    } catch (IllegalArgumentException ex) {
                        b = (JanusGraphVertex) threadedTx.traversal().V(id2).next();
                    }
                }
                try {
                    a.addEdge(EDGE_LABEL, b);
                    edgeCount.getAndIncrement();
                } catch (SchemaViolationException ignored) {
                }
            });
            threadedTx.commit();


//            while ((line = reader.readLine()) != null) {
//                if (i.get() / (double) datasetLines > 0.2) break;  // testing limit
//                if (line.startsWith("#")) continue;
//                if (i.incrementAndGet() % 100000 == 1) {
//                    System.out.printf("%.2f%%\n", i.get() / (double) datasetLines * 100);
//                }
//                String[] splits = line.split("\\s+");
//                if (splits[0].equals(splits[1])) continue; //we don't allow self cycles
//                Long id1 = iDmanager.toVertexId(1 + Long.decode(splits[0]));
//                Long id2 = iDmanager.toVertexId(1 + Long.decode(splits[1]));
//
//                JanusGraphVertex a = (JanusGraphVertex) g.V(id1).tryNext().orElse(null);
//                if (a == null) {
//                    a = graph.addVertex(T.label, VERTEX_LABEL, T.id, id1);
//                    computeClusterHelper(clusters, clusterMapper, id1);
//                }
//                JanusGraphVertex b = (JanusGraphVertex) g.V(id2).tryNext().orElse(null);
//                if (b == null) {
//                    b = graph.addVertex(T.label, VERTEX_LABEL, T.id, id2);
//                    computeClusterHelper(clusters, clusterMapper, id2);
//                }
//                try {
//                    a.addEdge(EDGE_LABEL, b);
//                    edgeCount.getAndIncrement();
//                } catch (SchemaViolationException ignored) {
//                }
//            }
            System.out.println();
        }

        GraphTraversalSource g = graph.traversal();
        long vCount = g.V().count().next();

        System.out.println("VCount: " + vCount);
        System.out.println("Clusters populated to vertex count of: " + clusters.values().stream().mapToLong(Pair::getValue1).reduce(0, (left, right) -> left + right));
        System.out.println("Edge count: " + edgeCount);
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
        Random random = new Random(RANDOM_SEED) {
            @Override
            public synchronized boolean nextBoolean() {
                return super.nextBoolean();
            }

            @Override
            public synchronized double nextDouble() {
                return super.nextDouble();
            }
        };
        GraphTraversalSource g = graph.traversal();
        final GraphTraversalSource gg = g;

//        for (GraphTraversal<Vertex, Vertex> it = g.V().limit(500); it.hasNext(); ) { // TODO testing limit

        g.V().toStream().parallel().forEach(vertex -> {
            long degree = graph.traversal().V(vertex.id()).outE().count().next();
            avgDegree.getAndAdd(degree);
            vertexIdsDegrees.put((Long) vertex.id(), degree);
            synchronized (gg) {
                if (degree > maxDegree.get()) maxDegree.set(degree);
            }
        });

        long vertexCount = vertexIdsDegrees.size();
//        long queryLimit = 50; // testing limit
        long queryLimit = vertexCount / 2000;
        avgDegree.set(avgDegree.get() / vertexCount);
        System.out.printf("Vertex count: %d, Max degree: %d, Avg. degree: %.2f\n", vertexCount, maxDegree.get(), avgDegree.get());

        List<Map.Entry<Long, Long>> vertexIdsDegreesList = new ArrayList<>(vertexIdsDegrees.entrySet());

        while (citiesToQuery.size() < queryLimit) {
            Map.Entry<Long, Long> pair = vertexIdsDegreesList.get(random.nextInt(Math.toIntExact(vertexCount)));
            if (Math.log(1 + pair.getValue()) / Math.log(1 + maxDegree.get()) > random.nextDouble() && !setCitiesToQuery.contains(pair.getKey())) {
                citiesToQuery.add(pair.getKey()); //add the vertex id to the list provided the probability
                setCitiesToQuery.add(pair.getKey());
            }
        }
        vertexIdsDegreesList.clear();
        vertexIdsDegrees.clear();
        random = new Random(RANDOM_SEED);
        GraphTraversalSource gn = graph.traversal();
        System.out.println("Number of queries: " + citiesToQuery.size());
        AtomicInteger noTargetCount = new AtomicInteger();
        final Random finalRandom = random;

        citiesToQuery.parallelStream().forEach(sourceId -> {
            int walkDistance = Math.min(randomWalkDistanceLimit, randomWalkDistanceLow + finalRandom.nextInt(randomWalkDistanceLimit));
//            System.out.println("walk distance: " + walkDistance);
            Optional<Vertex> target = gn.V(sourceId).repeat(__.both().simplePath().order().by(new ShuffleComparator<>(finalRandom)).limit(5)).times(walkDistance).limit(1)
                    .tryNext();

            if (!target.isPresent()) {
//                System.out.println("no target");
                noTargetCount.getAndIncrement();
                return;
            }
            long targetId = (long) target.get().id();
            queryData.put(new Pair<>(sourceId, targetId), (long) walkDistance);
            // get the shortest path between source and target
//            if (i % 10 == 0) System.out.printf("%.1f%%\n", i / (double) citiesToQuery.size() * 100);
        });


        queryData.entrySet().parallelStream().forEach(entry -> {
            long sourceId = entry.getKey().getValue0();
            long targetId = entry.getKey().getValue1();
            long walkDistance = entry.getValue();
            final GraphTraversalSource gt = log ? graph.traversal().withStrategies(ProcessedResultLoggingStrategy.instance()) : graph.traversal();
            gt.V(sourceId).
                    until(or(loops().is(walkDistance), hasId(targetId))).
                    repeat(out().simplePath()).hasId(targetId).path().limit(10).toStream().forEachOrdered(p -> {
//                System.out.println(Arrays.toString(p.objects().toArray()));
                Vertex previousO = null;
                for (Object o : p.objects()) {
                    if (previousO == null && o instanceof Vertex) {
                        MapHelper.incr(allExpandedVertices, (Long) ((Vertex) o).id(), 1L);
                        previousO = (Vertex) o;
                    } else if (o instanceof Vertex) {
                        MapHelper.incr(allExpandedVertices, (Long) ((Vertex) o).id(), 1L);
                        if (clusterMapper.map((Long) previousO.id()) != clusterMapper.map((Long) ((Vertex) o).id()))
                            originalCrossNodeQueries++;
                        else
                            originalNodeQueries++;
                        previousO = (Vertex) o;
//                        System.out.println(originalCrossNodeQueries);
                    }

                }
            });
        });
        System.out.println("No target count: " + noTargetCount);
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
        if (queryData.size() == 0)
            throw new Exception("The dataset runner must run the queries first before the result evaluation");
//TODO make same as runQueries
        GraphTraversalSource g = graph.traversal();
        for (Map.Entry<Pair<Long, Long>, Long> query : queryData.entrySet()) {
            long sourceId = query.getKey().getValue0();
            long targetId = query.getKey().getValue1();
            long walkDistance = query.getKey().getValue1();
            for (GraphTraversal<Vertex, org.apache.tinkerpop.gremlin.process.traversal.Path>
                 it = g.V(sourceId).
                    until(__.hasId(targetId).or().loops().is(walkDistance)).
                    repeat(__.out().simplePath()).hasId(targetId).path(); it.hasNext(); ) {
                org.apache.tinkerpop.gremlin.process.traversal.Path p = it.next();
                Vertex previousO = null;
                for (Object o : p.objects()) {
                    if (previousO == null && o instanceof Vertex) {
                        previousO = (Vertex) o;
                    } else if (o instanceof Vertex) {
                        if (!previousO.value(label).equals(((Vertex) o).value(label)))
                            repartitionedCrossNodeQueries++;
                        else
                            repartitionedNodeQueries++;
                        previousO = (Vertex) o;
                    }

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

    public Map<Long, Long> evaluatingMap() { //TODO
        return allExpandedVertices;
    }


}