package logHandling;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DefaultPRLogToGraphLoader implements PRLogToGraphLoader {
    @Override
    public boolean addSchema(StandardJanusGraph graph) {
        JanusGraphManagement management = graph.openManagement();
        boolean created;
        if (management.containsEdgeLabel(EDGE_LABEL)) {
            management.rollback();
            created = false;
        } else {
            PropertyKey lastUpdate = management.makePropertyKey("lastUpdate").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
            PropertyKey times = management.makePropertyKey("times").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
            management.makeEdgeLabel(EDGE_LABEL).unidirected().multiplicity(Multiplicity.SIMPLE).signature(times, lastUpdate).make();
            management.commit();
            created = true;
        }
        System.out.println(created ? "Successfully added the log graph schema" : "The log schema was already created");
        return created;
    }

    @Override
    public void removeSchema(StandardJanusGraph graph) {
        JanusGraphManagement management = graph.openManagement();
        boolean deleted = false;
        if (management.getEdgeLabel(EDGE_LABEL) != null) {
            management.getEdgeLabel(EDGE_LABEL).remove();
            deleted = true;
        }
        if (management.getPropertyKey("lastUpdate") != null) {
            management.getPropertyKey("lastUpdate").remove();
            deleted = true;
        }
        if (management.getPropertyKey("times") != null) {
            management.getPropertyKey("times").remove();
            deleted = true;
        }

        if (deleted) {
            management.commit();
            graph.close();
            System.out.println("Log schema cleared");
        }
    }

    @Override
    public void loadLogToGraph(StandardJanusGraph graph, Iterator<PRLogRecord> logRecords) {
        System.out.println("Loading the log to graph");
        GraphTraversalSource g = graph.traversal();
        Map<Pair<Long, Long>, Edge> edgeMap = new HashMap<>();
        long i = 0;
        for (; logRecords.hasNext(); ) {
            PRLogRecord lr = logRecords.next();
            i++;
//            if(i % 100 == 0 )  System.out.printf("%.2f%%\t\n", i / (double)log.logRecords.size() * 100);
            for (PRPath path : lr.results) {
                for (int s = 1; s < path.results.size(); s++) {
                    int f = s - 1;
                    Pair<Long, Long> p = new Pair<>(path.results.get(f).id, path.results.get(s).id);
                    Pair<Long, Long> pr = new Pair<>(path.results.get(s).id, path.results.get(f).id);
                    if (edgeMap.containsKey(p)) {
                        Edge e = edgeMap.get(p);
                        Long val = e.value("times");
                        Edge newEdge = g.E(e.id()).property("times", val + 1)
                                .property("lastUpdate", System.currentTimeMillis())
                                .next();
                        edgeMap.put(p, newEdge);
                        edgeMap.put(pr, newEdge);
                    } else {
                        try {
                            Edge e = g.V(path.results.get(f).id).addE(EDGE_LABEL)
                                    .property("times", 1)
                                    .property("lastUpdate", System.currentTimeMillis())
                                    .to(g.V(path.results.get(s).id)).next();
                            edgeMap.put(p, e);
                            edgeMap.put(pr, e);
                        } catch (Exception ignored) {
                        }
                    }
                }
            }
        }
        graph.tx().commit();
//        System.out.println(Iterators.size(g.E().hasLabel(EDGE_LABEL)));
        System.out.println("Edges added: " + edgeMap.size());
//        System.out.println("Edges overall: " + g.E().count().next());
    }
}
