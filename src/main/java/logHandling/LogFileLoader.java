package logHandling;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.util.*;
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
        return new MyLog(elements, records);
    }

    public static Iterator<LogRecord> loadIterator(File f) throws FileNotFoundException {
        final FileReader fileReader = new FileReader(f);
        final BufferedReader buffer = new BufferedReader(fileReader);
        final GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(LogRecord.class, new LogRecord.LogRecordDeserializer());
        final Gson gson = gsonBuilder.create();

        return new Iterator<LogRecord>() {
            LogRecord next = null;
            String line = null;

            @Override
            public synchronized boolean hasNext() {
                if (next != null) return true;
                try {
                    if ((line = buffer.readLine()) == null) {
                        buffer.close();
                        return false;
                    }
                } catch (IOException e) {
                    return false;
                }
                next = gson.fromJson(line, LogRecord.class);
                return true;
            }

            @Override
            public synchronized LogRecord next() {
                if (next != null || hasNext()) {
                    LogRecord tmp = next;
                    next = null;
                    return tmp;
                }
                throw new NoSuchElementException("no next");
            }
        };

    }
}
