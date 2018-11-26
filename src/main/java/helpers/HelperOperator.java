package helpers;



import org.javatuples.Pair;

import java.util.Map;
import java.util.function.BinaryOperator;

public enum HelperOperator implements BinaryOperator<Object> {
    /**
     * This operator will increment the value of every key in Map a that is present in map b by the the value of that key in map b.
     * Only second value of the Pair is incremented, the first value stays the same.
     */
    incrementPairMap {
        @Override
        public Object apply(Object a, Object b) {
            if (a instanceof Map && b instanceof Map)
                for (Map.Entry<Object, Pair<Long,Long>> entry : ((Map<Object, Pair<Long,Long>>) b).entrySet()) {
                    ((Map) a).computeIfAbsent(entry.getKey(), o -> entry.getValue());
                    ((Map<Object, Pair<Long,Long>>) a).computeIfPresent(entry.getKey(), (o, o2) -> new Pair<>(o2.getValue0(),o2.getValue1()+entry.getValue().getValue1()));
                }

            return a;
        }
    }
}
