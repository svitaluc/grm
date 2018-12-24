package logHandling;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class PRLogRecord {
    public String query;
    public List<PRPath> results;

    public static final class LogRecordDeserializer implements JsonDeserializer<PRLogRecord> {

        @Override
        public PRLogRecord deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
            final JsonObject jsonObject = jsonElement.getAsJsonObject();
            JsonElement q = jsonObject.get("Q");
            String query = q == null ? null : q.getAsString();
            List<PRPath> results = new ArrayList<>();
            JsonArray paths = jsonObject.get("R").getAsJsonArray();
            for (JsonElement path : paths) {
                JsonArray elements = path.getAsJsonArray();
                List<PRElement> lPath = new ArrayList<>();
                for (JsonElement el : elements) {
                    JsonElement value = el.getAsJsonObject().get("v");
                    String lType = "v";
                    if (value == null) {
                        value = el.getAsJsonObject().get("e");
                        lType = "e";
                    }
                    final long lId = value.getAsLong();
                    lPath.add(new PRElement(lId, lType));
                }
                results.add(new PRPath(lPath));

            }
            return new PRLogRecord(query, results);
        }
    }

    public PRLogRecord(String query, List<PRPath> results) {
        this.query = query;
        this.results = results;
    }
}
