package helpers;// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import logHandling.LogRecord;
import logHandling.MyElement;
import logHandling.MyLog;
import logHandling.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.*;

/**
 * Example Graph factory that creates a {@link JanusGraph} based on roman mythology.
 * Used in the documentation examples and tutorials.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ProcessedResultLogGraphFactory {
    public static final String EDGE_LABEL = "queriedTogether";


    @Deprecated
    public static String createStringSchemaQuery() {
        final StringBuilder s = new StringBuilder();

        s.append("JanusGraphManagement management = graph.openManagement(); ");
        s.append("boolean created = false; ");

        // naive check if the schema was previously created
        s.append(
                "if (management.getRelationTypes(RelationType.class).iterator().hasNext()) { management.rollback(); created = false; } else { ");

        // properties
        s.append("PropertyKey vid = management.makePropertyKey(\"vid\").dataType(String.class).cardinality(Cardinality.SINGLE).make(); ");
        s.append("PropertyKey eid = management.makePropertyKey(\"eid\").dataType(String.class).cardinality(Cardinality.SINGLE).make(); ");
        s.append("PropertyKey nodeId = management.makePropertyKey(\"nodeId\").dataType(Long.class).cardinality(Cardinality.LIST).make(); ");
        s.append("PropertyKey lastUpdate = management.makePropertyKey(\"lastUpdate\").dataType(Long.class).cardinality(Cardinality.SINGLE).make(); ");
        s.append("PropertyKey times = management.makePropertyKey(\"times\").dataType(Integer.class).cardinality(Cardinality.SINGLE).make(); ");


        // vertex labels
        s.append("management.makeVertexLabel(\"vertex\").signature(vid,nodeId).make(); ");

        // edge labels
        s.append("management.makeEdgeLabel(\"queriedTogether\").signature(eid,lastUpdate,times).make(); ");

        // composite indexes
        s.append("management.buildIndex(\"vidIndex\", Vertex.class).addKey(vid).buildCompositeIndex(); ");
        s.append("management.buildIndex(\"eidIndex\", Edge.class).addKey(eid).buildCompositeIndex(); ");

        s.append("management.commit(); created = true; }");

        return s.toString();
    }

    public static boolean createSchemaQuery(StandardJanusGraph graph) {
        JanusGraphManagement management = graph.openManagement();
        boolean created;
        if (management.getRelationTypes(RelationType.class).iterator().hasNext()) {
            management.rollback();
            created = false;
        } else {
            PropertyKey vid = management.makePropertyKey("vid").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            PropertyKey eid = management.makePropertyKey("eid").dataType(String.class).cardinality(Cardinality.SINGLE).make();
            PropertyKey nodeId = management.makePropertyKey("nodeIds").dataType(Long.class).cardinality(Cardinality.LIST).make();
            PropertyKey lastUpdate = management.makePropertyKey("lastUpdate").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
            PropertyKey times = management.makePropertyKey("times").dataType(Long.class).cardinality(Cardinality.SINGLE).make();

            management.makeVertexLabel("monitoredVertex").make();
            management.makeEdgeLabel("queriedTogether").multiplicity(Multiplicity.SIMPLE).signature(eid, lastUpdate, times).make();
            management.buildIndex("vidIndex", Vertex.class).addKey(vid).buildCompositeIndex();
            management.buildIndex("eidIndex", Edge.class).addKey(eid).buildCompositeIndex();
            management.commit();
            created = true;
        }
        return created;
    }

    public static void createElements(final GraphTraversalSource g, MyLog log) {
        // vertices
        LinkedHashMap<String, Vertex> vertexMap = new LinkedHashMap<>();
        List<MyElement> elements = new ArrayList<>(log.elements);
        for (MyElement el : elements) {
            if (el.type.equals("v")) {
                vertexMap.put(
                        el.getTypeId(),
                        g.addV("monitoredVertex")
                                .property("vid", el.getTypeId()).next());
//                                .property("nodeIds", el.physicalNodes).next());
            }
        }
        // edges
        Map<String, Edge> edgeMap = new HashMap<>();
        for (LogRecord logRec : log.logRecords) {
            for (Path path : logRec.results) {
                List<MyElement> results = path.results;
                for (int i = 0; i < results.size() - 1; i += 2) {
                    final String vId1 = results.get(i).getTypeId();
                    final String eId = results.get(i + 1).getTypeId();
                    final String vId2 = results.get(i + 2).getTypeId();
                    final Vertex v1 = vertexMap.get(vId1);
                    final Vertex v2 = vertexMap.get(vId2);
                    Edge e = edgeMap.get(eId);
                    if (e == null) {
                        Edge newEdge = g.V(v1.id()).addE(EDGE_LABEL)
                                .property("times", 1)
                                .property("eid", eId)
                                .property("lastUpdate", System.currentTimeMillis())
                                .to(g.V(v2.id()))
                                .next();
                        edgeMap.put(eId, newEdge);
                    } else {
                        Long val = e.value("times");
                        Edge newEdge = g.E(e.id()).property("times", val + 1)
                                .property("lastUpdate", System.currentTimeMillis())
                                .next();
                        edgeMap.put(eId, newEdge);
                    }

                }
            }
        }
        // commit the transaction to disk
        g.tx().commit();
    }
}
