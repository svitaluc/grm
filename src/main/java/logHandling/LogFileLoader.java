package logHandling;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class LogFileLoader {
    public static PRLog load(File f) throws IOException {
        FileReader fileReader = new FileReader(f);
        BufferedReader buffer = new BufferedReader(fileReader);
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(PRLogRecord.class, new PRLogRecord.LogRecordDeserializer());
        Gson gson = gsonBuilder.create();
        ArrayList<PRLogRecord> records = new ArrayList<>();
        Set<PRElement> elements = new HashSet<>();
        String line;
        while ((line = buffer.readLine()) != null) {
            PRLogRecord rec = gson.fromJson(line, PRLogRecord.class);
            records.add(rec);
            elements.addAll(rec.results.stream().flatMap(path -> path.results.stream()).collect(Collectors.toList()));
        }
        return new PRLog(elements, records);
    }

    public static Iterator<PRLogRecord> loadIterator(File f) throws FileNotFoundException {
        final FileReader fileReader = new FileReader(f);
        final BufferedReader buffer = new BufferedReader(fileReader);
        final GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(PRLogRecord.class, new PRLogRecord.LogRecordDeserializer());
        final Gson gson = gsonBuilder.create();

        return new Iterator<PRLogRecord>() {
            PRLogRecord next = null;
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
                next = gson.fromJson(line, PRLogRecord.class);
                return true;
            }

            @Override
            public synchronized PRLogRecord next() {
                if (next != null || hasNext()) {
                    PRLogRecord tmp = next;
                    next = null;
                    return tmp;
                }
                throw new NoSuchElementException("no next");
            }
        };

    }
}
