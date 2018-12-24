package helpers;


import org.javatuples.Pair;

import java.util.Map;
import java.util.function.BinaryOperator;

/**
 * Adds more operators to be used for binary reduction used in the memory of {@link partitioningAlgorithms.VaqueroVertexProgram}
 */
public enum HelperOperator implements BinaryOperator<Object> {
    /**
     * This operator will increment the value of every key in Map a that is present in map b by the the value of that key in map b.
     * Only second value of the Pair is incremented, the first value stays the same.
     */
    incrementPairMap {
        @Override
        public Object apply(Object a, Object b) {
            if (a instanceof Map && b instanceof Map)
                for (Map.Entry<Object, Pair<Long, Long>> entry : ((Map<Object, Pair<Long, Long>>) b).entrySet()) {
                    ((Map) a).computeIfAbsent(entry.getKey(), o -> entry.getValue());
                    ((Map<Object, Pair<Long, Long>>) a).computeIfPresent(entry.getKey(), (o, o2) -> new Pair<>(o2.getValue0(), o2.getValue1() + entry.getValue().getValue1()));
                }

            return a;
        }
    },
    /**
     * Thi operator will sum the values of map a and map b.
     */
    sumMap {
        @Override
        public Object apply(Object a, Object b) {
            if (a instanceof Map && b instanceof Map)
                for (Map.Entry<Object, Long> entry : ((Map<Object, Long>) b).entrySet()) {
                    ((Map) a).computeIfAbsent(entry.getKey(), o -> entry.getValue());
                    ((Map<Object, Long>) a).computeIfPresent(entry.getKey(), (o, o2) -> o2 + entry.getValue());
                }

            return a;
        }
    },
    /**
     * Thi operator will sum the values of Pair a and B.
     */
    sumPair {
        @Override
        public synchronized Object apply(Object a, Object b) {
            if (a instanceof Pair && b instanceof Pair)
                return new Pair<Long, Long>(((Pair<Long, Long>) a).getValue0() + ((Pair<Long, Long>) b).getValue0()
                        , ((Pair<Long, Long>) a).getValue1() + ((Pair<Long, Long>) b).getValue1()
                );
            return a;
        }
    }
}
