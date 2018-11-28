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
import java.util.stream.Collectors;

//TODO check WorkerExecutor
public class VaqueroVertexProgram extends StaticVertexProgram<Triplet<Serializable, Long, Long>> {

    private MessageScope.Local<Triplet<Serializable, Long, Long>> voteScope = MessageScope.Local.of(() -> __.bothE() // __.bothE(EDGE_LABEL) //TODO how to deal with disconnected components in the context of the EDGE_LABEL
            , (m, edge) -> {
                try {
                    m.setAt2(edge.<Long>value(EDGE_PROPERTY));
                } catch (IllegalStateException e) {
                    m.setAt2(-1L);
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
    private static final String EDGE_PROPERTY = "times";
    private static final String CLUSTER_MAPPER = "gremlin.VaqueroVertexProgram.clusterMapper";
    private static final String EVALUATING_SET = "gremlin.VaqueroVertexProgram.evaluatingSet";
    private static final String EVALUATING_STATS = "gremlin.VaqueroVertexProgram.evaluatingStats";
    private static final String EVALUATING_STATS_ORIGINAL = "gremlin.VaqueroVertexProgram.evaluatingStatsOriginal";
    private static final long RANDOM_SEED = 123456L;
    private Random random = new Random(RANDOM_SEED);

    private Graph graph;
    private long maxIterations = 100L;
    private long vertexCount = 0L;
    private int clusterCount = 16;
    double initialTemperature = 2D;//TODO
    double temperature = initialTemperature;//TODO
    double coolingFactor = .95D;//TODO
    Set<Long> evaluatingSet = new HashSet<>();
    Pair<Long, Long> evaluatingStatsOriginal;
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
            MemoryComputeKey.of(CLUSTER_LOWER_BOUND_SPACE, HelperOperator.sumMap, true, false),
            MemoryComputeKey.of(EVALUATING_STATS, HelperOperator.sumPair, false, false)
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
            memory.set(EVALUATING_STATS, new Pair<Long, Long>(0L, 0L));
            System.out.println("Clusters INIT Lower Bound: " + Arrays.toString(initialClusterLowerBoundSpace.entrySet().toArray()));
        }

    }

    @Override
    public void execute(Vertex vertex, Messenger<Triplet<Serializable, Long, Long>> messenger, Memory memory) {
        if (memory.isInitialIteration()) {
            this.graph = vertex.graph();
            if (areMockedPartitions)
                vertex.property(VertexProperty.Cardinality.single, LABEL, (long) random.nextInt(clusterCount));
            else {
                vertex.property(VertexProperty.Cardinality.single, LABEL, clusterMapper.map((Long) vertex.id()));
            }
            messenger.sendMessage(voteScope, new Triplet<>((Serializable) vertex.id(), vertex.<Long>value(LABEL), -1L));
        } else {
            final long VID = (Long) vertex.id();
            final long oldPID = vertex.<Long>value(LABEL);
            final Map<Long, Long> labels = new HashMap<>();
            final Map<Long, Long> evalLabels = new HashMap<>();
            Iterator<Triplet<Serializable, Long, Long>> rcvMsgs = messenger.receiveMessages();
            //count label frequency
            rcvMsgs.forEachRemaining(msg -> {
                MapHelper.incr(labels, msg.getValue1(), Math.abs(msg.getValue2()));
                MapHelper.incr(evalLabels, msg.getValue1(), (msg.getValue2() < 0) ? 1L : 0L);
            });
            //get most frequent label
            Long mfLabel;
            if (evaluatingSet.contains(VID)) {//Update evaluation metrics
                memory.add(EVALUATING_STATS, evaluateVertexCrossQuery(oldPID, evalLabels));
            }
            if (random.nextDouble() < getLabelSwitchProbability()) {
                if (labels.size() > 0) {
                    final LinkedHashMap<Long, Long> sortedByCount = labels.entrySet()
                            .stream()
                            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
                    int index = (int) Math.floor(random.nextDouble() * (temperature / initialTemperature) * (labels.size() - 1));
                    mfLabel = (Long) sortedByCount.keySet().toArray()[index];
//                    mfLabel = Collections.max(labels.entrySet(), Comparator.comparingLong(Map.Entry::getValue)).getKey(); //the old way
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
            messenger.sendMessage(voteScope, new Triplet<>((Serializable) vertex.id(), vertex.<Long>value(LABEL), -1L));
        }
    }

    @Override
    public boolean terminate(Memory memory) {
        int iteration = memory.getIteration();
        Pair<Long, Long> cStats = memory.get(EVALUATING_STATS);
        double tRatio = temperature / initialTemperature;
        if (evaluatingSet.size() > 0) {
            double improvement = (evaluatingStatsOriginal.getValue1() - cStats.getValue1()) / (double) evaluatingStatsOriginal.getValue1();
            if (memory.isInitialIteration()) {
                System.out.println("It.\tTemp\ttRatio\tImpr.\tpSwitch\tGood\tCross\t Sum");
            } else {
                System.out.printf("%d\t%.3f\t%.2f\t%.3f\t%.2f\t%d\t%d\t%d\n", iteration, temperature, tRatio, improvement, getLabelSwitchProbability(), cStats.getValue0(), cStats.getValue1(), cStats.getValue0() + cStats.getValue1());
            }
        } else {
            System.out.printf("End of Vaquero iteration: %d\t Temp: %.4f, tRatio: %.3f \n", iteration, temperature, tRatio);
        }
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT) || iteration >= this.maxIterations;
        if (voteToHalt) {
            System.out.printf("Terminated Vaquero algorithm at iteration: %d\n", iteration);
            return true;
        } else {
            memory.set(VOTE_TO_HALT, true); // need to reset to TRUE for the second and later iterations because of the binary AND operator
            memory.set(CLUSTER_UPPER_BOUND_SPACE, computeNewClusterUpperBoundSpace(memory.get(CLUSTERS))); // compute new cluster available space for next iteration
            memory.set(EVALUATING_STATS, new Pair<Long, Long>(0L, 0L)); //reset the counter for good and cross-cluster queries for evaluation
            temperature = getTemperature(iteration);
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
        this.maxIterations = configuration.getInt(MAX_ITERATIONS, 100);
        this.clusterCount = configuration.getInt(CLUSTER_COUNT, 16);
        this.acquireLabelProbability = configuration.getDouble(ACQUIRE_LABEL_PROBABILITY, 0.5);
        this.imbalanceFactor = configuration.getDouble(IMBALANCE_FACTOR, 0.9);
        this.initialClusters = Collections.synchronizedMap((Map<Long, Pair<Long, Long>>) configuration.getProperty(CLUSTERS));
        this.areMockedPartitions = configuration.getBoolean(ARE_MOCKED_PARTITIONS, false);
        this.clusterMapper = (ClusterMapper) configuration.getProperty(CLUSTER_MAPPER);
        this.initialClusterUpperBoundSpace = computeNewClusterUpperBoundSpace(initialClusters);
        this.initialClusterLowerBoundSpace = computeClusterLowerBoundSpace(initialClusters);
        this.evaluatingStatsOriginal = Pair.fromIterable((Iterable<Long>) configuration.getProperty(EVALUATING_STATS_ORIGINAL));
        this.evaluatingSet = Collections.synchronizedSet(new HashSet<Long>((Collection<? extends Long>) configuration.getProperty(EVALUATING_SET)));
        this.graph = graph;
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
        configuration.setProperty(EVALUATING_STATS_ORIGINAL, this.evaluatingStatsOriginal);
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
        long sumCapacity = cls.values().stream().mapToLong(Pair::getValue0).reduce(0, (left, right) -> left + right);
        return
                Collections.synchronizedMap(new HashMap<Long, Long>() {
                    {
                        for (Map.Entry<Long, Pair<Long, Long>> entry : cls.entrySet()) {
                            put(entry.getKey(), getClusterLowerBoundSpace(entry.getValue(), sumCapacity, vertexCount));
                        }
                    }
                });
    }

    private long getClusterUpperBoundSpace(Pair<Long, Long> capUsage) {
        return (capUsage.getValue0() - capUsage.getValue1()) / (clusterCount - 1);
    }

    private long getClusterLowerBoundSpace(Pair<Long, Long> cluster, long sumCapacity, long vertexCount) {
        return (long) (cluster.getValue1() - (imbalanceFactor * vertexCount * (cluster.getValue0() / (double) sumCapacity)));
    }

    private double getTemperature(int iteration) {
        return initialTemperature * Math.pow(coolingFactor, iteration);
    }

    private Pair<Long, Long> evaluateVertexCrossQuery(long PID, Map<Long, Long> labels) {
        long crossQuerries = 0;
        long goodQuerries = 0;
        for (Map.Entry<Long, Long> entry : labels.entrySet()) {
            if (entry.getKey().equals(PID))
                goodQuerries += entry.getValue();
            else
                crossQuerries += entry.getValue();
        }
        return new Pair<>(goodQuerries, crossQuerries);
    }

    private double getLabelSwitchProbability() {
        return acquireLabelProbability + (acquireLabelProbability * temperature / initialTemperature);
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

        public VaqueroVertexProgram.Builder evaluatingSet(List<Long> evaluatingSet) {
            this.configuration.setProperty(EVALUATING_SET, evaluatingSet);
            return this;
        }

        public VaqueroVertexProgram.Builder evaluatingStatsOriginal(Pair<Long, Long> evaluatingStats) {
            this.configuration.setProperty(EVALUATING_STATS_ORIGINAL, evaluatingStats);
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
