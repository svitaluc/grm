package logHandling;

import java.util.List;
import java.util.Set;

public class PRLog {
    public final Set<PRElement> elements;
    public final List<PRLogRecord> logRecords;

    public PRLog(Set<PRElement> elements, List<PRLogRecord> logRecords) {
        this.elements = elements;
        this.logRecords = logRecords;
    }
}
