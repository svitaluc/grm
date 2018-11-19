package logHandling;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class LogFileLoader {
    public static MyLog load(File f) throws IOException {
        FileReader fileReader = new FileReader(f);
        BufferedReader buffer = new BufferedReader(fileReader);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(LogRecord.class, new LogRecord.LogRecordDeserializer());
        Gson gson = gsonBuilder.create();
        ArrayList<LogRecord> records = new ArrayList<>();
        Set<MyElement> elements = new HashSet<>();
        String line;
        while ((line = buffer.readLine()) != null) {
            LogRecord rec = gson.fromJson(line, LogRecord.class);
            records.add(rec);
            elements.addAll(rec.results.stream().flatMap(path -> path.results.stream()).collect(Collectors.toList()));
        }
        return new MyLog(elements,records);
    }
}
