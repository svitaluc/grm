import java.util.List;
import java.util.Set;

public class MyLog {
    public final Set<MyElement> elements;
    public final List<LogRecord> logRecords;

    public MyLog(Set<MyElement> elements, List<LogRecord> logRecords) {
        this.elements = elements;
        this.logRecords = logRecords;
    }
}
