package helpers;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.LogPathStrategy;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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

    private static final String VERTEX_LABEL = "TwitterUser";
    private static final String EDGE_LABEL = "follows";
    private static final String LOG_EDGE_LABEL = "follows";

    public TwitterDatasetLoaderQueryRunner(String path) {
        this.datasetPath = Paths.get(path);
    }

    private boolean createSchemaQuery(StandardJanusGraph graph) {
        JanusGraphManagement management = graph.openManagement();
        boolean created;
        if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
            management.rollback();
            created = false;
        } else {
            management.makeVertexLabel("twitterUser").make();
            management.makeEdgeLabel("follows").directed().multiplicity(Multiplicity.SIMPLE).make();
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
            if (i > 50) break; //TODO remove this limit
            System.out.printf("%.2f%%\t%s\n", i / filesToScan.size() * 100, file.getName());
            Long id = iDmanager.toVertexId(Long.decode(file.getName().split("\\.")[0]));
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                JanusGraphVertex ego = (JanusGraphVertex) g.V(id).tryNext().orElse(null);
                if (ego == null) {
                    ego = graph.addVertex(T.label, VERTEX_LABEL, T.id, id);
                    computeClusterHelper(clusters,clusterMapper,id);
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
                        computeClusterHelper(clusters,clusterMapper,id1);
                    }

                    JanusGraphVertex b = (JanusGraphVertex) g.V(id2).tryNext().orElse(null);
                    if (b == null) {
                        b = graph.addVertex(T.label, VERTEX_LABEL, T.id, id2);
                        computeClusterHelper(clusters,clusterMapper,id2);
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
        return clusters;
    }

    void computeClusterHelper(Map<Long,Pair<Long,Long>> clusters, ClusterMapper clusterMapper, long id){
        clusters.compute(clusterMapper.map(id),(k,v)->{
            if(v==null){
                return new Pair<>(20000000L, 1L);
            }else
                return new Pair<>(20000000L,v.getValue1()+1);
        });
    }

    @Override
    public void runQueries(StandardJanusGraph graph) {
        System.out.println("Running test queries");
        Random random = new Random(123456);
        GraphTraversalSource g = graph.traversal();
        List<Long> tweetsReaders = new ArrayList<>();
        final List<Pair<Long, Long>> vertexIdsDegrees = new ArrayList<>();
        long maxDegree = 0;
        double avgDegree = 0;
        for (GraphTraversal<Vertex, Vertex> it = g.V().limit(50); it.hasNext(); ) { //TODO remove limit
            Vertex vertex = it.next();
            long degree = Iterators.size(vertex.edges(Direction.OUT));
            avgDegree += degree;
            vertexIdsDegrees.add(new Pair<>((Long) vertex.id(), degree));
            if (degree > maxDegree) maxDegree = degree;
        }

        long vertexCount = vertexIdsDegrees.size();
        long queryLimit = 50; //vertexCount / 3;
        avgDegree /= vertexCount;

        System.out.printf("Vertex count: %d, Max degree: %d, Avg. degree: %.2f\n", vertexCount, maxDegree, avgDegree);
        while (tweetsReaders.size() < queryLimit) {
            Pair<Long, Long> pair = vertexIdsDegrees.get(random.nextInt(Math.toIntExact(vertexCount)));
            if (pair.getValue1() / 1d / vertexCount > random.nextDouble()) {
                tweetsReaders.add(pair.getValue0()); //add the vertex id to the list provided the probability
            }
        }

        g = graph.traversal().withStrategies(LogPathStrategy.instance()); // enable the logging strategy
//        g = graph.traversal().withStrategies();
        for (Long vid : tweetsReaders) {
            boolean expandedNeighbour = false;
            for (GraphTraversal<Vertex, Vertex> it = g.V(vid).out(); it.hasNext(); ) {
                Vertex vertex = it.next();
                if (!expandedNeighbour && random.nextDouble() > 1 / avgDegree) {
                    Iterators.size(g.V(vertex.id()).out());
                    expandedNeighbour = true;
                }
            }
//            DefaultGraphTraversal<Vertex, Vertex> it = (DefaultGraphTraversal<Vertex, Vertex>) g.V(vid).outE().inV();
//            GraphTraversal<Vertex, org.apache.tinkerpop.gremlin.process.traversal.Path> it2 = it.clone().path();
//            System.out.println("b" + Arrays.toString(((Traversal.Admin<?, ?>) it2).getSteps().toArray()));
//            System.out.println(Arrays.toString(it.getStrategies().toList().toArray()));
//            it.applyStrategies();
//            System.out.println("a" + Arrays.toString(it.getSteps().toArray()));

//            for (GraphTraversal<Vertex, org.apache.tinkerpop.gremlin.process.traversal.Path> it = g.V(vid).outE().inV().path(); it.hasNext(); ) {
//                org.apache.tinkerpop.gremlin.process.traversal.Path path = it.next();
//                System.out.println(Arrays.toString(path.objects().toArray()));
//            }

        }


    }
}
