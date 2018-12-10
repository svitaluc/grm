package helpers;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
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
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class PenssylvaniaDatasetLoaderQueryRunner implements DatasetLoader, DatasetQueryRunner {
    private Path datasetPath;
    private final Map<Long, Long> vertexIdsDegrees = new HashMap<>();
    private final List<Long> citiesToQuery = new ArrayList<>();
    private final Set<Long> setCitiesToQuery = new HashSet<>();
    private final Map<Long, Long> allExpandedVertices = new HashMap<>();
    private long maxDegree = 0;
    private double avgDegree = 0;
    private long originalCrossNodeQueries = 0;
    private long originalNodeQueries = 0;
    private long repartitionedCrossNodeQueries = 0;
    private long repartitionedNodeQueries = 0;
    private static final long datasetLines = 3083800;
    private static final int randomWalkDistanceLimit = 10;
    private static final int randomWalkDistanceLow = 5;
    private Map<Pair<Long, Long>, Long> queryData = new HashMap<>();
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
        Map<Long, Pair<Long, Long>> clusters = new HashMap<>();
        createSchemaQuery(graph);
        GraphTraversalSource g = graph.traversal();
        g.tx().open();
        IDManager iDmanager = graph.getIDManager();
        System.out.println("Loading dataset to DB");
        long edgeCount = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(datasetPath.toFile()))) {
            String line;
            long i = 0;
            while ((line = reader.readLine()) != null) {
//                if (i > 100000) break;  // testing limit
                if (line.startsWith("#")) continue;
                if (++i % 100000 == 1) {
                    System.out.printf("%.2f%%\n", i / (double) datasetLines * 100);
                }
                String[] splits = line.split("\\s+");
                if (splits[0].equals(splits[1])) continue; //we don't allow self cycles
                Long id1 = iDmanager.toVertexId(1 + Long.decode(splits[0]));
                Long id2 = iDmanager.toVertexId(1 + Long.decode(splits[1]));

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
                try {
                    a.addEdge(EDGE_LABEL, b);
                    edgeCount++;
                } catch (SchemaViolationException ignored) {
                }
            }
            System.out.println();
        }

        g.tx().commit();
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
        Random random = new Random(RANDOM_SEED);
        GraphTraversalSource g = graph.traversal();


//        for (GraphTraversal<Vertex, Vertex> it = g.V().limit(500); it.hasNext(); ) { // TODO testing limit
        for (GraphTraversal<Vertex, Vertex> it = g.V(); it.hasNext(); ) {
            Vertex vertex = it.next();
            long degree = Iterators.size(vertex.edges(Direction.BOTH));
            avgDegree += degree;
            vertexIdsDegrees.put((Long) vertex.id(), degree);
            if (degree > maxDegree) maxDegree = degree;
        }
        long vertexCount = vertexIdsDegrees.size();
//        long queryLimit = 50; // testing limit
        long queryLimit = vertexCount / 1000;
        avgDegree /= vertexCount;
        System.out.printf("Vertex count: %d, Max degree: %d, Avg. degree: %.2f\n", vertexCount, maxDegree, avgDegree);

        List<Map.Entry<Long, Long>> vertexIdsDegreesList = new ArrayList<>(vertexIdsDegrees.entrySet());

        while (citiesToQuery.size() < queryLimit) {
            Map.Entry<Long, Long> pair = vertexIdsDegreesList.get(random.nextInt(Math.toIntExact(vertexCount)));
            if (Math.log(1 + pair.getValue()) / Math.log(1 + maxDegree) > random.nextDouble() && !setCitiesToQuery.contains(pair.getKey())) {
                citiesToQuery.add(pair.getKey()); //add the vertex id to the list provided the probability
                setCitiesToQuery.add(pair.getKey());
            }
        }
        random = new Random(RANDOM_SEED);
        GraphTraversalSource gn = graph.traversal();
        if (log)
            g = graph.traversal().withStrategies(LogPathStrategy.instance()); // enable the logging strategy
        else
            g = gn; // disabled logging

        System.out.println("Number of queries: " + citiesToQuery.size());
        int noTargetCount = 0;
        for (int i = 0; i < citiesToQuery.size(); i++) {
            long sourceId = citiesToQuery.get(i);
            int walkDistance = Math.min(randomWalkDistanceLimit, randomWalkDistanceLow + random.nextInt(randomWalkDistanceLimit));
//            System.out.println("walk distance: " + walkDistance);
            Optional<Vertex> target = gn.V(sourceId).repeat(__.both().simplePath().order().by(new ShuffleComparator<>(random)).limit(5)).times(walkDistance).limit(1)
                    .tryNext();

            if (!target.isPresent()) {
//                System.out.println("no target");
                noTargetCount++;
                continue;
            }
            long targetId = (long) target.get().id();
            queryData.put(new Pair<>(sourceId, targetId), (long) walkDistance);
            // get the shortest path between source and target
            if (i % 10 == 0) System.out.printf("%.1f%%\n", i / (double) citiesToQuery.size() * 100);
//            System.out.println(citiesToQuery.get(i) + " " + targetId);

            for (GraphTraversal<Vertex, org.apache.tinkerpop.gremlin.process.traversal.Path>
                 it = g.V(sourceId).
                    until(or(loops().is(walkDistance), hasId(targetId))).
                    repeat(out().simplePath()).hasId(targetId).path().limit(10); it.hasNext();
                    ) {
                org.apache.tinkerpop.gremlin.process.traversal.Path p = it.next();
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
            }
        }
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

    public Map<Long, Long> evaluatingMap() {
        return allExpandedVertices;
    }


}
