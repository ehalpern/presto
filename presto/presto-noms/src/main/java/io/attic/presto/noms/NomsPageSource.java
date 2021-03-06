/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.attic.presto.noms;

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.attic.presto.noms.ngql.ColumnQuery;
import io.attic.presto.noms.ngql.RowQuery;

import javax.json.JsonObject;
import javax.json.JsonValue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.stream.Collectors.toList;

public class NomsPageSource
        implements ConnectorPageSource
{
    private final NomsSchema schema;
    private final String tableName;
    private final TupleDomain<NomsColumnHandle> predicate;
    private final List<NomsColumnHandle> columns;
    private final List<Type> columnTypes;
    private final int batchSize;
    private final long totalLimit;
    private final NomsSession session;
    private long offset;
    private long totalCount;
    private boolean finished;

    public NomsPageSource(
            NomsSession session,
            NomsSplit split,
            List<NomsColumnHandle> columns)
    {
        this.columns = columns;
        this.columnTypes = columns.stream().map(NomsColumnHandle::type).collect(toList());
        this.tableName = split.getTableName().getTableName();
        this.schema = session.querySchema(this.tableName);
        this.predicate = split.getEffectivePredicate();
        this.offset = split.getOffset();
        this.totalLimit = split.getLimit() <= 0 ? Long.MAX_VALUE : split.getLimit();
        this.batchSize = session.config().getBatchSize();
        this.session = session;
    }

    @Override
    public long getTotalBytes()
    {
        return totalCount;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalCount;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0L;
    }

    @Override
    public Page getNextPage()
    {
        switch (schema.tableStructure()) {
            case ColumnMajor:
                return getNextPageColumnMajor();
            case RowMajor:
                return getNextPageRowMajor();
            default:
                throw new AssertionError("Unexpected table structure: " + schema.tableStructure());
        }
    }

    private Page getNextPageColumnMajor()
    {
        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        if (totalCount % batchSize != 0 || totalCount == totalLimit) {
            // last batch request exhausted rows
            finished = true;
        }
        else {
            long limit = Math.min(batchSize, totalLimit - totalCount);
            ColumnQuery query = ColumnQuery.create(schema, columns, predicate, offset, limit);
            ColumnQuery.Result result = session.execute(tableName, query);
            finished = result.size() == 0;

            if (!finished) {
                pageBuilder.declarePositions(result.size());
                for (int i = 0; i < columns.size(); i++) {
                    NomsColumnHandle col = columns.get(i);
                    BlockBuilder builder = pageBuilder.getBlockBuilder(i);
                    Type type = col.type();
                    Class<?> javaType = col.type().getJavaType();
                    if (type == BooleanType.BOOLEAN) {
                        for (boolean b : result.columnOfBooleans(col.name())) {
                            type.writeBoolean(builder, b);
                        }
                    }
                    else if (type == DoubleType.DOUBLE) {
                        for (double d : result.columnOfDoubles(col.name())) {
                            type.writeDouble(builder, d);
                        }
                    }
                    else if (type == VarcharType.VARCHAR) {
                        for (String s : result.columnOfStrings(col.name())) {
                            type.writeSlice(builder, utf8Slice(s));
                        }
                    }
                    else {
                        throw new PrestoException(NOT_SUPPORTED, "type:" + javaType);
                    }
                }
                offset += batchSize;
                totalCount += result.size();
            }
        }
        return pageBuilder.build();
    }

    private Page getNextPageRowMajor()
    {
        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        if (totalCount % batchSize != 0 || totalCount == totalLimit) {
            // last batch request exhausted rows
            finished = true;
        }
        else {
            long limit = Math.min(batchSize, totalLimit - totalCount);
            RowQuery query = RowQuery.create(schema, columns, predicate, offset, limit);
            RowQuery.Result result = session.execute(tableName, query);
            finished = result.size() == 0;

            Iterator<JsonValue> rows = result.rows();
            while (rows.hasNext()) {
                pageBuilder.declarePosition();
                JsonObject row = rows.next().asJsonObject();
                for (int i = 0; i < columns.size(); i++) {
                    NomsColumnHandle col = columns.get(i);
                    BlockBuilder builder = pageBuilder.getBlockBuilder(i);
                    Type type = col.type();
                    Class<?> javaType = col.type().getJavaType();
                    if (type == BooleanType.BOOLEAN) {
                        type.writeBoolean(builder, row.get(col.name()) == JsonValue.TRUE);
                    }
                    else if (type == DoubleType.DOUBLE) {
                        type.writeDouble(builder, row.getJsonNumber(col.name()).doubleValue());
                    }
                    else if (type == VarcharType.VARCHAR) {
                        type.writeSlice(builder, utf8Slice(row.getJsonString(col.name()).getString()));
                    }
                    else {
                        throw new PrestoException(NOT_SUPPORTED, "type:" + javaType);
                    }
                }
                offset += batchSize;
                totalCount += result.size();
            }
        }
        return pageBuilder.build();
    }

    @Override
    public void close()
            throws IOException
    {
        // nothing to do
    }
}
