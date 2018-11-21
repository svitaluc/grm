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
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.*;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

//TODO check WorkerExecutor
public class VacqueroVertexProgram extends StaticVertexProgram<Pair<Serializable, Long>> {

    private MessageScope.Local<?> voteScope = MessageScope.Local.of(() -> __.bothE(EDGE_LABEL)); //TODO how to deal with disconnected components in the context of the EDGE_LABEL

    public static final String LABEL = "gremlin.VaqueroVertexProgram.label";
    public static final String ARE_MOCKED_PARTITIONS = "gremlin.VaqueroVertexProgram.areMockedPartitions";
    public static final String CLUSTER_COUNT = "gremlin.VaqueroVertexProgram.clusterCount";
    public static final String CLUSTERS = "gremlin.VaqueroVertexProgram.clusters";
    public static final String ACQUIRE_LABEL_PROBABILITY = "gremlin.VaqueroVertexProgram.acquireLabelProbability";
    private static final String VOTE_TO_HALT = "gremlin.VaqueroVertexProgram.voteToHalt";
    private static final String MAX_ITERATIONS = "gremlin.VaqueroVertexProgram.maxIterations";
    private static final String EDGE_LABEL = "queriedTogether";
    private static final String CLUSTER_MAPPER = "gremlin.VaqueroVertexProgram.clusterMapper";
    private Random random = new Random();

    private long maxIterations = 30;
    private int clusterCount = 16;
    //custer label -> (capacity, usage) TODO check if properly stored in memory by CLUSTERS key
    private Map<Long, Pair<Long, Long>> initialClusters = null;
    private double acquireLabelProbability = 0.5;
    private boolean areMockedPartitions = false;
    private ClusterMapper clusterMapper;

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true),
            MemoryComputeKey.of(CLUSTERS, Operator.addAll, true, false) //TODO check if the values are not overwritten
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

    private VacqueroVertexProgram() {
    }

    @Override
    public void setup(Memory memory) {
        if (memory.isInitialIteration()) {
            memory.set(VOTE_TO_HALT, false);
            memory.set(CLUSTERS, initialClusters);
        }

    }

    @Override
    public void execute(Vertex vertex, Messenger<Pair<Serializable, Long>> messenger, Memory memory) {
        if (memory.isInitialIteration()) {
            if (areMockedPartitions)
                vertex.property(VertexProperty.Cardinality.single, LABEL, (long) random.nextInt(clusterCount));
            else {
                vertex.property(VertexProperty.Cardinality.single, LABEL, clusterMapper.map((Long) vertex.id()));
            }
        } else if (1 == memory.getIteration()) { //first iteration - there is on message receiving
            messenger.sendMessage(voteScope, new Pair<>((Serializable) vertex.id(), vertex.<Long>value(LABEL)));
        } else {
            final Map<Long, Long> labels = new HashMap<>();
            Iterator<Pair<Serializable, Long>> rcvMsgs = messenger.receiveMessages();
            if (random.nextDouble() > 1 - acquireLabelProbability) {
                //count label frequency
                rcvMsgs.forEachRemaining(msg -> {
                    AtomicReference<Edge> edge = new AtomicReference<>();
                    vertex.edges(Direction.BOTH, EDGE_LABEL).forEachRemaining((e) -> {
                        AtomicBoolean thatEdge = new AtomicBoolean(false);
                        e.bothVertices().forEachRemaining(vertex1 -> thatEdge.set(vertex1.id().equals(msg.getValue0())));
                        if (thatEdge.get()) edge.set(e);
                    });
                    MapHelper.incr(labels, msg.getValue1(), edge.get().<Long>value("times"));
                });
                //get most frequent label
                Long mfLabel;
                if(labels.size()>0) {
                    mfLabel = Collections.max(labels.entrySet(), Comparator.comparingLong(Map.Entry::getValue)).getKey();
                }else {
                    mfLabel = vertex.<Long>value(LABEL);
                }
                if (mfLabel.equals(vertex.<Long>value(LABEL))) { //label is the same - voting to halt the program
                    memory.add(VOTE_TO_HALT, true);
                } else {// acquiring new most frequent label - voting to continue the program
                    boolean acquired = acquireNewLabel(vertex.<Long>value(LABEL), mfLabel, memory, vertex);
                    vertex.property(VertexProperty.Cardinality.single, LABEL, mfLabel);
                    memory.add(VOTE_TO_HALT, !acquired);//vote according to the result of acquireNewLabel()

                }
            } else {
                memory.add(VOTE_TO_HALT, true);// Not acquiring label so voting to halt the program
            }
            //sending the label to neighbours
            messenger.sendMessage(voteScope, new Pair<>((Serializable) vertex.id(), vertex.<Long>value(LABEL)));
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


    public static VacqueroVertexProgram.Builder build() {
        return new VacqueroVertexProgram.Builder();
    }


    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        this.maxIterations = configuration.getInt(MAX_ITERATIONS, 30);
        this.clusterCount = configuration.getInt(CLUSTER_COUNT, 16);
        this.acquireLabelProbability = configuration.getDouble(ACQUIRE_LABEL_PROBABILITY, 0.5);
        this.initialClusters = (Map<Long, Pair<Long, Long>>) configuration.getProperty(CLUSTERS);
        this.areMockedPartitions = configuration.getBoolean(ARE_MOCKED_PARTITIONS, false);
        this.clusterMapper = (ClusterMapper) configuration.getProperty(CLUSTER_MAPPER);
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
        long available = newClusterCapacityUsage.getValue0() - newClusterCapacityUsage.getValue1() / clusterCount;
        if (available > 0) { // checking upper partition space upper bound, TODO implement lower bound check
            memory.add(CLUSTERS, new HashMap<Long, Pair<Long, Long>>() {{
                put(oldClusterId, new Pair<>(oldClusterCapacityUsage.getValue0(), oldClusterCapacityUsage.getValue1() - 1));
                put(newClusterId, new Pair<>(newClusterCapacityUsage.getValue0(), newClusterCapacityUsage.getValue1() + 1));
            }});
            vertex.property(VertexProperty.Cardinality.single, LABEL, newClusterId);
            return true;
        }
        return false;
    }

    //------------------------------------------------------------------------------------------------------------------
    public static final class Builder extends AbstractVertexProgramBuilder<VacqueroVertexProgram.Builder> {


        private Builder() {
            super(VacqueroVertexProgram.class);
        }

        public VacqueroVertexProgram.Builder maxIterations(final int iterations) {
            this.configuration.setProperty(MAX_ITERATIONS, iterations);
            return this;
        }

        //for testing purpose
        public VacqueroVertexProgram.Builder areMockedPartitions(boolean value) {
            this.configuration.setProperty(ARE_MOCKED_PARTITIONS, value);
            return this;
        }

        //injects map of cluster to capacity and usage
        public VacqueroVertexProgram.Builder clusters(Map<Long, Pair<Long, Long>> clusters) {
            this.configuration.setProperty(CLUSTERS, clusters);
            this.configuration.setProperty(CLUSTER_COUNT, clusters.size());
            return this;
        }

        //injects clusterMapper
        public VacqueroVertexProgram.Builder clusterMapper(ClusterMapper clusterMapper) {
            this.configuration.setProperty(CLUSTER_MAPPER, clusterMapper);
            this.configuration.setProperty(ARE_MOCKED_PARTITIONS, false);
            return this;
        }

        public VacqueroVertexProgram.Builder acquireLabelProbability(final double probability) {
            this.configuration.setProperty(ACQUIRE_LABEL_PROBABILITY, probability);
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
