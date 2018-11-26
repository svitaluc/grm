/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package partitioningAlgorithms;

import helpers.ClusterMapper;
import helpers.HelperOperator;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.Serializable;
import java.util.*;

//TODO check WorkerExecutor
public class VaqueroVertexProgram extends StaticVertexProgram<Triplet<Serializable, Long, Long>> {

    private MessageScope.Local<Triplet<Serializable, Long, Long>> voteScope = MessageScope.Local.of(() -> __.bothE() // __.bothE(EDGE_LABEL) //TODO how to deal with disconnected components in the context of the EDGE_LABEL
            , (m, edge) -> {
                try {
                    m.setAt2(edge.value(EDGE_LABEL));
                } catch (IllegalArgumentException e) {
                    m.setAt2(1L);
                }
                return m;
            });

    public static final String LABEL = "gremlin.VaqueroVertexProgram.label";
    public static final String ARE_MOCKED_PARTITIONS = "gremlin.VaqueroVertexProgram.areMockedPartitions";
    public static final String CLUSTER_COUNT = "gremlin.VaqueroVertexProgram.clusterCount";
    public static final String CLUSTERS = "gremlin.VaqueroVertexProgram.clusters";
    public static final String CLUSTER_UPPER_BOUND_SPACE = "gremlin.VaqueroVertexProgram.clusterUpperBoundSpace";
    public static final String CLUSTER_LOWER_BOUND_SPACE = "gremlin.VaqueroVertexProgram.clusterLowerBoundSpace";
    public static final String ACQUIRE_LABEL_PROBABILITY = "gremlin.VaqueroVertexProgram.acquireLabelProbability";
    public static final String IMBALANCE_FACTOR = "gremlin.VaqueroVertexProgram.imbalanceFactor";
    private static final String VOTE_TO_HALT = "gremlin.VaqueroVertexProgram.voteToHalt";
    private static final String MAX_ITERATIONS = "gremlin.VaqueroVertexProgram.maxIterations";
    private static final String EDGE_LABEL = "queriedTogether";
    private static final String CLUSTER_MAPPER = "gremlin.VaqueroVertexProgram.clusterMapper";
    private static final long RANDOM_SEED = 123456L;
    private Random random = new Random(RANDOM_SEED);

    private long maxIterations = 30L;
    private long vertexCount = 0L;
    private int clusterCount = 16;
    //custer label -> (capacity, usage) TODO check if properly stored in memory by CLUSTERS key
    private Map<Long, Pair<Long, Long>> initialClusters = null;
    private Map<Pair<Long, Long>, Long> initialClusterUpperBoundSpace = null;
    private Map<Long, Long> initialClusterLowerBoundSpace = null;
    private double acquireLabelProbability = 0.5;
    private double imbalanceFactor = 0.9;
    private boolean areMockedPartitions = false;
    private ClusterMapper clusterMapper;

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true),
            MemoryComputeKey.of(CLUSTERS, HelperOperator.incrementPairMap, true, false),
            MemoryComputeKey.of(CLUSTER_UPPER_BOUND_SPACE, HelperOperator.sumMap, true, false),
            MemoryComputeKey.of(CLUSTER_LOWER_BOUND_SPACE, HelperOperator.sumMap, true, false)
    ));
    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            VertexComputeKey.of(LABEL, false) //TODO check if the values are not overwritten
    ));

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return VERTEX_COMPUTE_KEYS;
    }

    private VaqueroVertexProgram() {
    }

    @Override
    public void setup(Memory memory) {
        if (memory.isInitialIteration()) {
            memory.set(VOTE_TO_HALT, false);
            memory.set(CLUSTERS, initialClusters);
            memory.set(CLUSTER_UPPER_BOUND_SPACE, initialClusterUpperBoundSpace);
            memory.set(CLUSTER_LOWER_BOUND_SPACE, initialClusterLowerBoundSpace);
            System.out.println("Clusters INIT Lower Bound: "+Arrays.toString(initialClusterLowerBoundSpace.entrySet().toArray()));
        }

    }

    @Override
    public void execute(Vertex vertex, Messenger<Triplet<Serializable, Long, Long>> messenger, Memory memory) {
        if (memory.isInitialIteration()) {
            if (areMockedPartitions)
                vertex.property(VertexProperty.Cardinality.single, LABEL, (long) random.nextInt(clusterCount));
            else {
                vertex.property(VertexProperty.Cardinality.single, LABEL, clusterMapper.map((Long) vertex.id()));
            }
            messenger.sendMessage(voteScope, new Triplet<>((Serializable) vertex.id(), vertex.<Long>value(LABEL), 1L));
        } else {
            final long oldPID = vertex.<Long>value(LABEL);
            final Map<Long, Long> labels = new HashMap<>();
            Iterator<Triplet<Serializable, Long, Long>> rcvMsgs = messenger.receiveMessages();
            if (random.nextDouble() > 1 - acquireLabelProbability) {
                //count label frequency
                rcvMsgs.forEachRemaining(msg -> {
//                    AtomicReference<Edge> edge = new AtomicReference<>();
//                    vertex.edges(Direction.BOTH, EDGE_LABEL).forEachRemaining((e) -> {
//                        AtomicBoolean thatEdge = new AtomicBoolean(false);
//                        e.bothVertices().forEachRemaining(vertex1 -> thatEdge.set(vertex1.id().equals(msg.getValue0())));
//                        if (thatEdge.get()) edge.set(e);
//                    });
//                    MapHelper.incr(labels, msg.getValue1(), edge.get().<Long>value("times"));
                    MapHelper.incr(labels, msg.getValue1(), msg.getValue2());
                });
                //get most frequent label
                Long mfLabel;
                if (labels.size() > 0) {
                    mfLabel = Collections.max(labels.entrySet(), Comparator.comparingLong(Map.Entry::getValue)).getKey();
                } else {
                    mfLabel = oldPID;
                }
                if (mfLabel.equals(oldPID)) { //label is the same - voting to halt the program
                    memory.add(VOTE_TO_HALT, true);
                } else {// acquiring new most frequent label - voting to continue the program
                    boolean acquired = acquireNewLabel(oldPID, mfLabel, memory, vertex);
                    memory.add(VOTE_TO_HALT, !acquired);//vote according to the result of acquireNewLabel()

                }
            } else {
                memory.add(VOTE_TO_HALT, true);// Not acquiring label so voting to halt the program
            }
            //sending the label to neighbours
            messenger.sendMessage(voteScope, new Triplet<>((Serializable) vertex.id(), vertex.<Long>value(LABEL), 0L));
        }
    }

    @Override
    public boolean terminate(Memory memory) {
        System.out.printf("End of Vacquero iteration: %d\n", memory.getIteration());
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT) || memory.getIteration() >= this.maxIterations;
        if (voteToHalt) {
            System.out.printf("Terminated Vacquero algorithm at iteration: %d\n", memory.getIteration());
            return true;
        } else {
            if (memory.getIteration() > 1)
                memory.set(VOTE_TO_HALT, true); // need to reset to TRUE for the second and later iterations because of the binary AND operator
            memory.set(CLUSTER_UPPER_BOUND_SPACE, computeNewClusterUpperBoundSpace(memory.get(CLUSTERS))); // compute new cluster available space for next iteration
            return false;
        }
    }


    @Override
    public Set<MessageScope> getMessageScopes(Memory memory) {
        return new HashSet<>(Collections.singletonList(this.voteScope));
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.ORIGINAL;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.VERTEX_PROPERTIES;
    }


    public static VaqueroVertexProgram.Builder build() {
        return new VaqueroVertexProgram.Builder();
    }


    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.vertexCount = graph.traversal().V().count().next();

        this.maxIterations = configuration.getInt(MAX_ITERATIONS, 30);
        this.clusterCount = configuration.getInt(CLUSTER_COUNT, 16);
        this.acquireLabelProbability = configuration.getDouble(ACQUIRE_LABEL_PROBABILITY, 0.5);
        this.imbalanceFactor = configuration.getDouble(IMBALANCE_FACTOR, 0.9);
        this.initialClusters = Collections.synchronizedMap((Map<Long, Pair<Long, Long>>) configuration.getProperty(CLUSTERS));
        this.areMockedPartitions = configuration.getBoolean(ARE_MOCKED_PARTITIONS, false);
        this.clusterMapper = (ClusterMapper) configuration.getProperty(CLUSTER_MAPPER);
        this.initialClusterUpperBoundSpace = computeNewClusterUpperBoundSpace(initialClusters);
        this.initialClusterLowerBoundSpace = computeClusterLowerBoundSpace(initialClusters);
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(MAX_ITERATIONS, this.maxIterations);
        configuration.setProperty(CLUSTER_COUNT, this.clusterCount);
        configuration.setProperty(ACQUIRE_LABEL_PROBABILITY, this.acquireLabelProbability);
        configuration.setProperty(CLUSTERS, this.initialClusters);
        configuration.setProperty(ARE_MOCKED_PARTITIONS, this.areMockedPartitions);
        configuration.setProperty(CLUSTER_MAPPER, this.clusterMapper);
    }


    //
    private boolean acquireNewLabel(long oldClusterId, long newClusterId, Memory memory, Vertex vertex) {
        Pair<Long, Long> oldClusterCapacityUsage = memory.<Map<Long, Pair<Long, Long>>>get(CLUSTERS).get(oldClusterId);
        Pair<Long, Long> newClusterCapacityUsage = memory.<Map<Long, Pair<Long, Long>>>get(CLUSTERS).get(newClusterId);
        long available = memory.<Map<Pair<Long, Long>, Long>>get(CLUSTER_UPPER_BOUND_SPACE).get(new Pair<>(oldClusterId, newClusterId));
        if (available > 0) { // checking upper partition space upper and lowerBound
            long remaining = memory.<Map<Long, Long>>get(CLUSTER_LOWER_BOUND_SPACE).get(oldClusterId);
            if (remaining > 0) {
                memory.add(CLUSTERS, Collections.synchronizedMap(new HashMap<Long, Pair<Long, Long>>() {{
                    put(oldClusterId, new Pair<>(oldClusterCapacityUsage.getValue0(), -1L));
                    put(newClusterId, new Pair<>(newClusterCapacityUsage.getValue0(), 1L));
                }}));

                memory.add(CLUSTER_UPPER_BOUND_SPACE, Collections.synchronizedMap(new HashMap<Pair<Long, Long>, Long>() {{
                    put(new Pair<>(oldClusterId, newClusterId), -1L);
                }}));

                memory.add(CLUSTER_LOWER_BOUND_SPACE, Collections.synchronizedMap(new HashMap<Long, Long>() {{
                    put(oldClusterId, -1L);
                    put(newClusterId, 1L);
                }}));

                vertex.property(VertexProperty.Cardinality.single, LABEL, newClusterId);
                return true;
            }
        }
        return false;
    }

    private Map<Pair<Long, Long>, Long> computeNewClusterUpperBoundSpace(Map<Long, Pair<Long, Long>> cls) {
        return
                Collections.synchronizedMap(new HashMap<Pair<Long, Long>, Long>() {
                    {
                        for (Map.Entry<Long, Pair<Long, Long>> entry : cls.entrySet()) {
                            for (Map.Entry<Long, Pair<Long, Long>> entry2 : cls.entrySet()) {
                                if (entry.getKey().equals(entry2.getKey())) continue;
                                put(new Pair<>(entry.getKey(), entry2.getKey()), getClusterUpperBoundSpace(entry.getValue()));
                            }
                        }
                    }
                });
    }

    private Map<Long, Long> computeClusterLowerBoundSpace(Map<Long, Pair<Long, Long>> cls) {
        long sumCapacity = cls.values().stream().mapToLong(Pair::getValue0).reduce(0,(left, right) -> left+right);
        return
                Collections.synchronizedMap(new HashMap<Long, Long>() {
                    {
                        for (Map.Entry<Long, Pair<Long, Long>> entry : cls.entrySet()) {
                                put(entry.getKey(), getClusterLowerBoundSpace(entry.getValue(),sumCapacity,vertexCount));
                        }
                    }
                });
    }

    private long getClusterUpperBoundSpace(Pair<Long, Long> capUsage) {
        return (capUsage.getValue0() - capUsage.getValue1()) / (clusterCount - 1);
    }
    private long getClusterLowerBoundSpace(Pair<Long,Long> cluster, long sumCapacity, long vertexCount) {
        return (long) (cluster.getValue1() - (imbalanceFactor*vertexCount*(cluster.getValue0()/(double)sumCapacity)));
    }

    //------------------------------------------------------------------------------------------------------------------
    public static final class Builder extends AbstractVertexProgramBuilder<VaqueroVertexProgram.Builder> {


        private Builder() {
            super(VaqueroVertexProgram.class);
        }

        public VaqueroVertexProgram.Builder maxIterations(final int iterations) {
            this.configuration.setProperty(MAX_ITERATIONS, iterations);
            return this;
        }

        //for testing purpose
        public VaqueroVertexProgram.Builder areMockedPartitions(boolean value) {
            this.configuration.setProperty(ARE_MOCKED_PARTITIONS, value);
            return this;
        }

        //injects map of cluster to capacity and usage
        public VaqueroVertexProgram.Builder clusters(Map<Long, Pair<Long, Long>> clusters) {
            this.configuration.setProperty(CLUSTERS, clusters);
            this.configuration.setProperty(CLUSTER_COUNT, clusters.size());
            return this;
        }

        //injects clusterMapper
        public VaqueroVertexProgram.Builder clusterMapper(ClusterMapper clusterMapper) {
            this.configuration.setProperty(CLUSTER_MAPPER, clusterMapper);
            this.configuration.setProperty(ARE_MOCKED_PARTITIONS, false);
            return this;
        }

        public VaqueroVertexProgram.Builder acquireLabelProbability(final double probability) {
            this.configuration.setProperty(ACQUIRE_LABEL_PROBABILITY, probability);
            return this;
        }

        public VaqueroVertexProgram.Builder imbalanceFactor(final double factor) {
            this.configuration.setProperty(IMBALANCE_FACTOR, factor);
            return this;
        }


    }

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresLocalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }


        };
    }
}
