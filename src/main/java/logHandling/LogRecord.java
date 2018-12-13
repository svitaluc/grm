package logHandling;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class LogRecord {
    public String query;
    public List<Path> results;

    public static final class LogRecordDeserializer implements JsonDeserializer<LogRecord> {

        @Override
        public LogRecord deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
            final JsonObject jsonObject = jsonElement.getAsJsonObject();
            JsonElement q = jsonObject.get("Q");
            String query = q == null ? null : q.getAsString();
            List<Path> results = new ArrayList<>();
            JsonArray paths = jsonObject.get("R").getAsJsonArray();
            for (JsonElement path : paths) {
                JsonArray elements = path.getAsJsonArray();
                List<MyElement> lPath = new ArrayList<>();
                for (JsonElement el : elements) {
                    JsonElement value = el.getAsJsonObject().get("v");
                    String lType = "v";
                    if (value == null) {
                        value = el.getAsJsonObject().get("e");
                        lType = "e";
                    }
                    final long lId = value.getAsLong();
                    lPath.add(new MyElement(lId, lType));
                }
                results.add(new Path(lPath));

            }
            return new LogRecord(query, results);
        }
    }

    public LogRecord(String query, List<Path> results) {
        this.query = query;
        this.results = results;
    }
}
