package com.zdjizhi.etl.utils;

import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.zdjizhi.base.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;

public class CsvReaderUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CsvReaderUtils.class);

    private CSVReader csvReader;

    private final Map<String, Integer> columnNames = new HashMap<>();

    private final List<Map<String, String>> data = new ArrayList<>();

    public CsvReaderUtils(Reader reader) {
        init(reader, new ArrayList<>());
    }

    public CsvReaderUtils(Reader reader, List<String> columns) {
        init(reader, columns);
    }

    private void init(Reader reader, List<String> columns) {
        try {
            this.csvReader = new CSVReader(reader, CSVParser.DEFAULT_SEPARATOR,
                    CSVParser.DEFAULT_QUOTE_CHARACTER, CSVParser.DEFAULT_ESCAPE_CHARACTER);
            String[] headers = csvReader.readNext();
            if (headers == null) {
                return;
            }
            for (int i = 0; i < headers.length; i++) {
                String columnName = headers[i];
                columnNames.put(columnName, i);
            }
            if (columns.size() == 0) {
                columns.addAll(columnNames.keySet());
            }
            Iterator<String[]> iterator = csvReader.iterator();
            while (iterator.hasNext()) {
                Map<String, String> lineMap = new HashMap<>();
                String[] next = iterator.next();
                for (String s : columns) {
                    Integer index = columnNames.get(s);
                    lineMap.put(s, next[index]);
                }
                data.add(lineMap);
            }
        } catch (Exception e) {
            System.out.println("csv parse error, error exception: " + e);
            e.printStackTrace(System.out);
        } finally {
            if (csvReader != null) {
                try {
                    csvReader.close();
                } catch (Exception ignored) {

                }
            }
        }
    }

    public List<Map<String, String>> readAll() {
        return data;
    }

    public Integer getFieldIndex(String fieldName) {
        return columnNames.get(fieldName);
    }

    private InputStreamReader getReader(byte[] bytes) {
        return new InputStreamReader(new ByteArrayInputStream(bytes));
    }

    private InputStreamReader getReader(String fileRelativePath) {
        byte[] bytes = FileUtils.getFileBytes(fileRelativePath);
        return getReader(bytes);
    }


}
