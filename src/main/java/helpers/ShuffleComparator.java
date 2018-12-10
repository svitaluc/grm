package helpers;

import java.util.Comparator;
import java.util.Random;

public class ShuffleComparator<T> implements Comparator<T> {
    Random random;

    public ShuffleComparator(Random random) {
        this.random = random;
    }

    @Override
    public int compare(T o1, T o2) {
        return random.nextBoolean() ? -1 : 1;
    }

    @Override
    public Comparator<T> reversed() {
        return this;
    }
}
