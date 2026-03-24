package io.r2dbc.yashandb;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * R2DBC {@link RowMetadata} implementation for YashanDB.
 */
public final class YashanDbRowMetadata implements RowMetadata {

    private final List<YashanDbColumnMetadata> columns;
    /** Case-insensitive column name → 0-based index. */
    private final Map<String, Integer> nameIndex;

    private YashanDbRowMetadata(List<YashanDbColumnMetadata> columns) {
        this.columns = Collections.unmodifiableList(columns);
        Map<String, Integer> idx = new HashMap<>(columns.size() * 2);
        for (int i = 0; i < columns.size(); i++) {
            // putIfAbsent: for duplicate column names, first occurrence wins
            idx.putIfAbsent(columns.get(i).getName().toLowerCase(), i);
        }
        this.nameIndex = Collections.unmodifiableMap(idx);
    }

    /**
     * Build from JDBC {@link ResultSetMetaData}.
     */
    static YashanDbRowMetadata fromJdbc(ResultSetMetaData rsmd) throws SQLException {
        int count = rsmd.getColumnCount();
        List<YashanDbColumnMetadata> cols = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            cols.add(YashanDbColumnMetadata.fromJdbc(rsmd, i));
        }
        return new YashanDbRowMetadata(cols);
    }

    @Override
    public ColumnMetadata getColumnMetadata(int index) {
        if (index < 0 || index >= columns.size()) {
            throw new IndexOutOfBoundsException("Column index " + index + " out of range [0, " + (columns.size() - 1) + "]");
        }
        return columns.get(index);
    }

    @Override
    public ColumnMetadata getColumnMetadata(String name) {
        Integer index = nameIndex.get(name.toLowerCase());
        if (index == null) {
            throw new NoSuchElementException("Column '" + name + "' not found");
        }
        return columns.get(index);
    }

    @Override
    public List<? extends ColumnMetadata> getColumnMetadatas() {
        return columns;
    }

    /**
     * Look up the 0-based index for a column name (case-insensitive).
     */
    int indexOf(String name) {
        Integer index = nameIndex.get(name.toLowerCase());
        if (index == null) {
            throw new NoSuchElementException("Column '" + name + "' not found");
        }
        return index;
    }

    int size() {
        return columns.size();
    }
}
