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

import cluster.PartitionMapper;
import helpers.HelperOperator;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.*;
import org.javatuples.Pair;
import org.javatuples.Quartet;

import java.io.Serializable;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static dataset.DatasetLoader.computeClusterHelper;

/**
 * Partitioning algorithm which sets a partition each vertex according to the partition frequency of that vertex's neighbours.
 * This algorithm is implemented as {@link VertexProgram} and ran against the {@link Graph} in parallel manner.
 * The structure of the implementation is based on the {@link org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram}.
 */
public class VaqueroVertexProgram extends StaticVertexProgram<Quartet<Serializable, Long, Long, Long>> {

    private MessageScope.Local<Quartet<Serializable, Long, Long, Long>> voteScope;

    /**
     * The key to a vertex property defining proposed new partition
     */
    public static final String PARTITION = "gremlin.VaqueroVertexProgram.partition";
    public static final String ARE_MOCKED_PARTITIONS = "gremlin.VaqueroVertexProgram.areMockedPartitions";
    /**
     * Memory key to store the number of
     */
    public static final String PARTITION_COUNT = "gremlin.VaqueroVertexProgram.partitionCount";
    /**
     * Memory key to store a Map<Long, Pair<Long, Long>> that keeps the ID of partition with its capacity and current usage.
     */
    public static final String CLUSTER = "gremlin.VaqueroVertexProgram.cluster";
    /**
     * Memory key to store a Map<Pair<Long, Long>, Long> that keeps the maximum amount of available vertex transfers from one partition
     * to another. The pair of these partitions acts as key to this map. This map is updated once per iteration.
     */
    public static final String CLUSTER_UPPER_BOUND_SPACE = "gremlin.VaqueroVertexProgram.clusterUpperBoundSpace";
    /**
     * Memory key to store Map<Long, Long> that keeps the maximum amount of vertices that can be transferred from a partition.
     * The ID of the partition is the key to this map. If the returned value of the partition ID is higher than zero then the
     * partition is eligible to transfer vertices from.
     */
    public static final String CLUSTER_LOWER_BOUND_SPACE = "gremlin.VaqueroVertexProgram.clusterLowerBoundSpace";
    /**
     * Configuration key to set the acquireLabelProbability variable. Which affects if and vertex should acquire new partition.
     */
    public static final String ACQUIRE_PARTITION_PROBABILITY = "gremlin.VaqueroVertexProgram.acquirePartitionProbability";
    /**
     * Configuration key to set the imbalanceFactor variable. Which affects the amount of eligible vertices to transfer in
     * the Memory variable CLUSTER_LOWER_BOUND_SPACE.
     */
    public static final String IMBALANCE_FACTOR = "gremlin.VaqueroVertexProgram.imbalanceFactor";
    /**
     * Configuration key to set the imbalanceFactor variable. Which affects the temperature equation that affects the
     * probability of acquiring new partition ID.
     */
    public static final String ADOPTION_FACTOR = "gremlin.VaqueroVertexProgram.adoptionFactor";
    /**
     * Memory key to boolean variable. Each vertex in their execute() run will vote true/false whether to halt the program or not.
     * The votes sent by all vertices are then reduced via {@link Operator#and} into a single value. If true at the end of
     * an iteration the program will halt.
     */
    private static final String VOTE_TO_HALT = "gremlin.VaqueroVertexProgram.voteToHalt";
    /**
     * Memory key to store the number of times the partitions were changed during an iteration.
     */
    private static final String VOTE_COUNT = "gremlin.VaqueroVertexProgram.voteCount";
    /**
     * Configuration key to set the maxIterations variable defining the maximum number of iterations before the program will halt.
     */
    private static final String MAX_ITERATIONS = "gremlin.VaqueroVertexProgram.maxIterations";
    private static final String MAX_PARTITION_CHANGES = "gremlin.VaqueroVertexProgram.maxPartitionChanges";
    /**
     * Configuration key to set the coolingFactor variable defining the rate of cooling down the temperature of simulated annealing.
     */
    private static final String COOLING_FACTOR = "gremlin.VaqueroVertexProgram.coolingFactor";
    /**
     * The key of an edge property storing the weight of the PR Log edge
     */
    private static final String EDGE_PROPERTY = "times";
    /**
     * Configuration key that sets the incidentTraversal variable. This determines the message scope.
     * __::bothE will send the message to neighbours of a vertex using all edges.
     * __::outE will send the message to neighbours of a vertex using only outgoing edges.
     */
    private static final String INCIDENT_TRAVERSAL = "gremlin.VaqueroVertexProgram.incidentTraversal";
    /**
     * Configuration key that sets the clusterMapper variable that is responsible for mapping the partition of given vertexID.
     */
    private static final String CLUSTER_MAPPER = "gremlin.VaqueroVertexProgram.clusterMapper";
    /**
     * Memory key to store Map of vertex IDs and the number of times the vertex was appeared in the PR Log. This is used
     * for evaluation of the partitioning improvement in between iterations. (only eligible for Twitter EGO dataset)
     */
    private static final String EVALUATING_MAP = "gremlin.VaqueroVertexProgram.evaluatingMap";
    /**
     * Configuration key to store a number of vertices that have different partition ID then in the beginning of the program.
     */
    private static final String EVALUATING_CLUSTER_SWITCH_COUNT = "gremlin.VaqueroVertexProgram.evaluatingClusterSwitchCount";
    /**
     * Memory key to store the cross-node and same-node communication of vertices in the EVALUATING_MAP map.
     * (only for Twitter EGO dataset)
     */
    private static final String EVALUATING_STATS = "gremlin.VaqueroVertexProgram.evaluatingStats";
    /**
     * Configuration key to the initial values of cross-node/same-node communication. Gives the ability to calculate improvement of EVALUATING_STATS.
     */
    private static final String EVALUATING_STATS_ORIGINAL = "gremlin.VaqueroVertexProgram.evaluatingStatsOriginal";
    /**
     * Configuration key to boolean variable to enable/disable the evaluation of cross-node/same-node communication.
     * Only the Twitter EGO dataset is eligible to use this feature.
     */
    private static final String EVALUATE_CROSS_COMMUNICATION = "gremlin.VaqueroVertexProgram.evaluateCrossCommunication";

    private static final long RANDOM_SEED = 123456L;
    private Random random = new Random(RANDOM_SEED);

    private long iterTime = 0;
    private long initTime = 0;
    private long maxIterations = 100L;
    private long maxPartitionChanges = 100L;
    private long vertexCount = 0L;
    private int partitionCount = 16;
    private double initialTemperature = 2D;
    private double adoptionFactor = 1D;
    private double temperature = initialTemperature;
    private double coolingFactor = .98D;
    private Map<Long, Long> evaluatingMap = new HashMap<>();
    private Pair<Long, Long> evaluatingStatsOriginal;
    //custer partition -> (capacity, usage)
    private Map<Long, Pair<Long, Long>> initialClusters = null;
    private Map<Pair<Long, Long>, Long> initialClusterUpperBoundSpace = null;
    private Map<Long, Long> initialClusterLowerBoundSpace = null;
    private double acquirePartitionProbability = 0.5;
    private double imbalanceFactor = 0.9;
    private boolean areMockedPartitions = false;
    private boolean evaluateCrossCommunication = false;
    private PartitionMapper clusterMapper;
    private Supplier<Traversal<Vertex, Edge>> incidentTraversal;

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true),
            MemoryComputeKey.of(VOTE_COUNT, Operator.sumLong, true, true),
            MemoryComputeKey.of(MAX_PARTITION_CHANGES, Operator.sumLong, true, true),
            MemoryComputeKey.of(EVALUATING_CLUSTER_SWITCH_COUNT, Operator.sumLong, false, true),
            MemoryComputeKey.of(CLUSTER, HelperOperator.incrementPairMap, true, false),
            MemoryComputeKey.of(CLUSTER_UPPER_BOUND_SPACE, HelperOperator.sumMap, true, false),
            MemoryComputeKey.of(CLUSTER_LOWER_BOUND_SPACE, HelperOperator.sumMap, true, false),
            MemoryComputeKey.of(EVALUATING_STATS, HelperOperator.sumPair, false, false)
    ));
    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            VertexComputeKey.of(PARTITION, false)
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
            iterTime = initTime = System.currentTimeMillis();
            memory.set(VOTE_TO_HALT, false);
            memory.set(CLUSTER, initialClusters);
            memory.set(CLUSTER_UPPER_BOUND_SPACE, initialClusterUpperBoundSpace);
            memory.set(CLUSTER_LOWER_BOUND_SPACE, initialClusterLowerBoundSpace);
            memory.set(EVALUATING_STATS, new Pair<Long, Long>(0L, 0L));
            memory.set(EVALUATING_CLUSTER_SWITCH_COUNT, 0L);
            memory.set(VOTE_COUNT, 0L);
            memory.set(MAX_PARTITION_CHANGES, maxPartitionChanges);
            System.out.printf("Staring Vaquero partitioning - cF=%.3f, iF=%.3f, nClusters=%d, aProb=%.2f\n", coolingFactor, imbalanceFactor, initialClusters.size(), acquirePartitionProbability);
            System.out.println("Clusters INIT Lower Bound: " + Arrays.toString(initialClusterLowerBoundSpace.entrySet().toArray()));
        }

    }

    @Override
    public void execute(Vertex vertex, Messenger<Quartet<Serializable, Long, Long, Long>> messenger, Memory memory) {
        if (memory.isInitialIteration()) {//initial iteration preparations
            if (areMockedPartitions)
                vertex.property(VertexProperty.Cardinality.single, PARTITION, (long) random.nextInt(partitionCount));
            else {
                vertex.property(VertexProperty.Cardinality.single, PARTITION, clusterMapper.map((Long) vertex.id()));
            }
            messenger.sendMessage(voteScope, new Quartet<>((Serializable) vertex.id(), vertex.value(PARTITION), -1L, -1L));

        } else {
            final long VID = (Long) vertex.id();
            final long oldPID = vertex.<Long>value(PARTITION);
            final Map<Long, Long> partitions = new HashMap<>();
            final Map<Long, Long> evalPartitions = new HashMap<>();
            Iterator<Quartet<Serializable, Long, Long, Long>> rcvMsgs = messenger.receiveMessages();
            //count partition frequency
            rcvMsgs.forEachRemaining(msg -> {
                MapHelper.incr(partitions, msg.getValue1(), Math.abs(msg.getValue2()));
                if (evaluateCrossCommunication)
                    MapHelper.incr(evalPartitions, msg.getValue1(), (msg.getValue2() < 0 && msg.getValue3().equals(VID)) ? 1L : 0L); // Twitter in between iterations evaluation
            });
            //get most frequent partition
            Long mfPartition;
            if (evaluatingMap.containsKey(VID)) {//Update evaluation metrics
                memory.add(EVALUATING_STATS, evaluateVertexCrossQuery(oldPID, evalPartitions, evaluatingMap.get(VID)));
            }
            if (random.nextDouble() < getPartitionAcquirementProbability()) {
                if (partitions.size() > 0) {
                    LinkedHashMap<Long, Long> sortedByCount = partitions.entrySet()
                            .stream()
                            .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
                    int index = (int) Math.floor(random.nextDouble() * (temperature / initialTemperature) * (partitions.size() - 1));
                    mfPartition = new LinkedList<>(sortedByCount.keySet()).get(index);
                } else {
                    mfPartition = oldPID;
                }
                if (mfPartition.equals(oldPID)) { //partition is the same - voting to halt the program
                    memory.add(VOTE_TO_HALT, true);
                } else {// acquiring new most frequent partition - voting to continue the program
                    boolean acquired = acquireNewPartition(oldPID, mfPartition, memory, vertex);
                    memory.add(VOTE_TO_HALT, !acquired);//vote according to the result of acquireNewPartition()
                }
            } else {
                memory.add(VOTE_TO_HALT, true);// Not acquiring partition so voting to halt the program
            }
            //sending the partition to neighbours
            messenger.sendMessage(voteScope, new Quartet<>((Serializable) vertex.id(), vertex.<Long>value(PARTITION), -1L, -1L));
            if (clusterMapper.map((Long) vertex.id()) != vertex.<Long>value(PARTITION)) {
                memory.add(EVALUATING_CLUSTER_SWITCH_COUNT, 1L);
            }
            partitions.clear();
        }
    }

    @Override
    public boolean terminate(Memory memory) {
        int iteration = memory.getIteration();
        Pair<Long, Long> cStats = memory.get(EVALUATING_STATS);
        Long cSwitch = memory.get(EVALUATING_CLUSTER_SWITCH_COUNT);
        Long cpCount = memory.get(VOTE_COUNT);
        double cpPercent = cpCount / (double) vertexCount * 100;
        double tRatio = temperature / initialTemperature;
        long now = System.currentTimeMillis();
        long time = now - iterTime;
        if (evaluatingMap.size() > 0) {
            double improvement = (evaluatingStatsOriginal.getValue1() - cStats.getValue1()) / (double) evaluatingStatsOriginal.getValue1();
            if (memory.isInitialIteration()) {
                System.out.printf("Cooling factor: %.3f\n", coolingFactor);
                System.out.println("It.\t\tTemp\t\ttRatio\t\tImpr.\t\tpSwitch\t\tGood\t\tCross\t\tSum\t\tsCount\t\tcpCount\t\tTime");
            } else {
                System.out.printf("%d\t\t%.3f\t\t%.2f\t\t%.3f\t\t%.2f\t\t%d\t\t%d\t\t%d\t\t%d\t\t%.2f\t\t%.2fs\t\t \n",
                        iteration, temperature, tRatio, improvement, getPartitionAcquirementProbability(), cStats.getValue0(), cStats.getValue1(), cStats.getValue0() + cStats.getValue1(), cSwitch, cpPercent, time / 1000D);
            }
        } else {
            System.out.printf("End of Vaquero iteration: %d\t Temp: %.4f, tRatio: %.3f \n", iteration, temperature, tRatio);
        }
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT) || iteration >= this.maxIterations;
        iterTime = now;
        if (voteToHalt) {
            System.out.printf("Terminated Vaquero algorithm at iteration: %d, Runtime: %.2f\n", iteration, (now - initTime) / 1000D);
            return true;
        } else {
            memory.set(VOTE_TO_HALT, true); // need to reset to TRUE for the second and later iterations because of the binary AND operator
            memory.set(CLUSTER_UPPER_BOUND_SPACE, computeNewClusterUpperBoundSpace(memory.get(CLUSTER))); // compute new cluster available space for next iteration
            memory.set(EVALUATING_STATS, new Pair<Long, Long>(0L, 0L)); //reset the counter for good and cross-cluster queries for evaluation
            memory.set(EVALUATING_CLUSTER_SWITCH_COUNT, 0L); //reset the counter for good and cross-cluster queries for evaluation
            memory.set(VOTE_COUNT, 0L); //reset the counter for number of partition changes
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
        this.acquirePartitionProbability = configuration.getDouble(ACQUIRE_PARTITION_PROBABILITY, 0.5);
        this.imbalanceFactor = configuration.getDouble(IMBALANCE_FACTOR, 0.9);
        this.coolingFactor = configuration.getDouble(COOLING_FACTOR, 0.98);
        this.adoptionFactor = configuration.getDouble(ADOPTION_FACTOR, 1D);
        this.areMockedPartitions = configuration.getBoolean(ARE_MOCKED_PARTITIONS, false);
        this.evaluateCrossCommunication = configuration.getBoolean(EVALUATE_CROSS_COMMUNICATION, false);
        this.clusterMapper = (PartitionMapper) configuration.getProperty(CLUSTER_MAPPER);
        this.evaluatingStatsOriginal = Pair.fromIterable((Iterable<Long>) configuration.getProperty(EVALUATING_STATS_ORIGINAL));

        this.evaluatingMap = (Map<Long, Long>) configuration.getProperty(EVALUATING_MAP);
        if (this.evaluatingMap == null) this.evaluatingMap = new HashMap<>();
        this.evaluatingMap = Collections.synchronizedMap((Map<Long, Long>) configuration.getProperty(EVALUATING_MAP));
        this.initialClusters = Collections.synchronizedMap(new HashMap<>());
        graph.traversal().V().toStream().parallel().forEach(v -> {
            try {
                long PID = v.value(PARTITION);
                computeClusterHelper(initialClusters, PID);
            } catch (Exception e) {
                computeClusterHelper(initialClusters, clusterMapper, (Long) v.id());
            }
        });

        this.partitionCount = initialClusters.size();
        this.initialClusterUpperBoundSpace = computeNewClusterUpperBoundSpace(initialClusters);
        this.initialClusterLowerBoundSpace = computeClusterLowerBoundSpace(initialClusters);
        this.incidentTraversal = (Supplier<Traversal<Vertex, Edge>>) configuration.getProperty(INCIDENT_TRAVERSAL);
        if (this.incidentTraversal == null) this.incidentTraversal = () -> __.bothE();
        this.voteScope = MessageScope.Local.of(this.incidentTraversal
                , (m, edge) -> {
                    try {
                        return new Quartet<>(m.getValue0(), m.getValue1(), edge.<Long>value(EDGE_PROPERTY), -1L);
                    } catch (Exception e) {
                        return new Quartet<>(m.getValue0(), m.getValue1(), -1L, edge.vertices(Direction.OUT).hasNext() ? (Long) edge.vertices(Direction.OUT).next().id() : -1);
                    }
                });
        this.maxPartitionChanges = (long) (vertexCount * configuration.getDouble(MAX_PARTITION_CHANGES, 1D));
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(MAX_ITERATIONS, this.maxIterations);
        configuration.setProperty(PARTITION_COUNT, this.partitionCount);
        configuration.setProperty(ACQUIRE_PARTITION_PROBABILITY, this.acquirePartitionProbability);
        configuration.setProperty(CLUSTER, this.initialClusters);
        configuration.setProperty(ARE_MOCKED_PARTITIONS, this.areMockedPartitions);
        configuration.setProperty(CLUSTER_MAPPER, this.clusterMapper);
        configuration.setProperty(COOLING_FACTOR, this.coolingFactor);
        configuration.setProperty(ADOPTION_FACTOR, this.adoptionFactor);
        configuration.setProperty(IMBALANCE_FACTOR, this.imbalanceFactor);
        configuration.setProperty(EVALUATING_STATS_ORIGINAL, this.evaluatingStatsOriginal);
        configuration.setProperty(INCIDENT_TRAVERSAL, this.incidentTraversal);
        configuration.setProperty(EVALUATE_CROSS_COMMUNICATION, this.evaluateCrossCommunication);
        configuration.setProperty(MAX_PARTITION_CHANGES, this.maxPartitionChanges);
    }


    /**
     * Calculates the probability of the acquirement of new partitionID of the vertex and proceeds with a coin toss to determine if the new id will be acquired.
     *
     * @param oldPartitionId
     * @param newPartitionId
     * @param memory         Memory of the computer
     * @param vertex         the given vertex
     * @return true of if the new newPartitionId was acquired, else otherwise
     */
    private boolean acquireNewPartition(long oldPartitionId, long newPartitionId, Memory memory, Vertex vertex) {
        Pair<Long, Long> oldClusterCapacityUsage = memory.<Map<Long, Pair<Long, Long>>>get(CLUSTER).get(oldPartitionId);
        Pair<Long, Long> newClusterCapacityUsage = memory.<Map<Long, Pair<Long, Long>>>get(CLUSTER).get(newPartitionId);
        long voteCount = memory.get(VOTE_COUNT);
        if (voteCount >= maxPartitionChanges) return false;
        long available = memory.<Map<Pair<Long, Long>, Long>>get(CLUSTER_UPPER_BOUND_SPACE).get(new Pair<>(oldPartitionId, newPartitionId));
        if (available > 0) { // checking upper partition space upper and lowerBound
            long remaining = memory.<Map<Long, Long>>get(CLUSTER_LOWER_BOUND_SPACE).get(oldPartitionId);
            if (remaining > 0) {
                memory.add(CLUSTER, Collections.synchronizedMap(new HashMap<Long, Pair<Long, Long>>() {{
                    put(oldPartitionId, new Pair<>(oldClusterCapacityUsage.getValue0(), -1L));
                    put(newPartitionId, new Pair<>(newClusterCapacityUsage.getValue0(), 1L));
                }}));

                memory.add(CLUSTER_UPPER_BOUND_SPACE, Collections.synchronizedMap(new HashMap<Pair<Long, Long>, Long>() {{
                    put(new Pair<>(oldPartitionId, newPartitionId), -1L);
                }}));

                memory.add(CLUSTER_LOWER_BOUND_SPACE, Collections.synchronizedMap(new HashMap<Long, Long>() {{
                    put(oldPartitionId, -1L);
                    put(newPartitionId, 1L);
                }}));

                vertex.property(VertexProperty.Cardinality.single, PARTITION, newPartitionId);
                memory.add(VOTE_COUNT, 1L);
                return true;
            }
        }
        return false;
    }

    /**
     * Computes the upper bound capacity limit of each partition for each other partition in the cluster.
     * This is the limit for one iteration.
     *
     * @param cls the cluster with partitions
     * @return Map of the partition upper bound capacity limit for each other partition
     */
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

    /**
     * Computes the lower bound capacity limit for each partition. This is computed only at the start of the partitions
     * and the returned values is updated when vertices acquire new partitionId.
     *
     * @param cls cluster's partitions capacity/usage values
     * @return
     */
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

    /**
     * Helper to get the available space of given partition.
     *
     * @param capUsage capacity and its usage.
     * @return the calculated space
     */
    private long getClusterUpperBoundSpace(Pair<Long, Long> capUsage) {
        return (capUsage.getValue0() - capUsage.getValue1()) / (partitionCount - 1);
    }

    /**
     * Helper to get the limit of minimum usage for each parition of the cluster
     *
     * @param partitionCapacityUsage capacity and current usage of partition
     * @param sumCapacity            cluster capacity sum
     * @param vertexCount            count of every vertex of the graph
     * @return the number of vertices that can leave given partitions
     */
    private long getClusterLowerBoundSpace(Pair<Long, Long> partitionCapacityUsage, long sumCapacity, long vertexCount) {
        return (long) (partitionCapacityUsage.getValue1() - (imbalanceFactor * vertexCount * (partitionCapacityUsage.getValue0() / (double) sumCapacity)));
    }

    /**
     * @param iteration iteration number
     * @return the temperature for the given iteration
     */
    private double getTemperature(int iteration) {
        return initialTemperature * Math.pow(coolingFactor, iteration);
    }

    /**
     * Helper to calculate cross node communication of vertices.
     *
     * @param PID        partition ID
     * @param partitions map of vertex ID to its partition ID
     * @param times      factor of communication frequency
     * @return the updated statistics (not cross, cross) pair
     */
    private Pair<Long, Long> evaluateVertexCrossQuery(long PID, Map<Long, Long> partitions, long times) {
        long crossQuerries = 0;
        long goodQuerries = 0;
        for (Map.Entry<Long, Long> entry : partitions.entrySet()) {
            if (entry.getKey().equals(PID))
                goodQuerries += entry.getValue();
            else
                crossQuerries += entry.getValue();
        }
        return new Pair<>(times * goodQuerries, times * crossQuerries);
    }

    /**
     * @return the probability of acquirement of new PID for a vertex.
     */
    private double getPartitionAcquirementProbability() {
        return acquirePartitionProbability + ((1 - acquirePartitionProbability) * temperature / initialTemperature) * adoptionFactor;
    }

    //------------------------------------------------------------------------------------------------------------------

    /**
     * Builder for creating and setting up and instance of {@link org.apache.tinkerpop.gremlin.process.computer.clustering.vacquero.VacqueroVertexProgram}.
     */
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

        public VaqueroVertexProgram.Builder evaluateCrossCommunication(boolean value) {
            this.configuration.setProperty(EVALUATE_CROSS_COMMUNICATION, value);
            return this;
        }

        //injects clusterMapper
        public VaqueroVertexProgram.Builder clusterMapper(PartitionMapper clusterMapper) {
            this.configuration.setProperty(CLUSTER_MAPPER, clusterMapper);
            this.configuration.setProperty(ARE_MOCKED_PARTITIONS, false);
            return this;
        }

        public VaqueroVertexProgram.Builder acquirePartitionProbability(final double probability) {
            this.configuration.setProperty(ACQUIRE_PARTITION_PROBABILITY, probability);
            return this;
        }

        public VaqueroVertexProgram.Builder imbalanceFactor(final double factor) {
            this.configuration.setProperty(IMBALANCE_FACTOR, factor);
            return this;
        }

        public VaqueroVertexProgram.Builder evaluatingMap(Map<Long, Long> evaluatingSet) {
            this.configuration.setProperty(EVALUATING_MAP, evaluatingSet);
            return this;
        }

        public VaqueroVertexProgram.Builder evaluatingStatsOriginal(Pair<Long, Long> evaluatingStats) {
            this.configuration.setProperty(EVALUATING_STATS_ORIGINAL, evaluatingStats);
            return this;
        }

        public VaqueroVertexProgram.Builder coolingFactor(double coolingFactor) {
            this.configuration.setProperty(COOLING_FACTOR, coolingFactor);
            return this;
        }

        public VaqueroVertexProgram.Builder adoptionFactor(double adoptionFactor) {
            this.configuration.setProperty(ADOPTION_FACTOR, adoptionFactor);
            return this;
        }

        public VaqueroVertexProgram.Builder maxPartitionChangeRatio(double percent) {
            this.configuration.setProperty(MAX_PARTITION_CHANGES, percent);
            return this;
        }

        public VaqueroVertexProgram.Builder scopeIncidentTraversal(Supplier<Traversal<Vertex, Edge>> traversal) {
            this.configuration.setProperty(INCIDENT_TRAVERSAL, traversal);
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
