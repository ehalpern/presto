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
package com.facebook.presto.noms;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.noms.NomsType.NUMBER;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;

public class NomsRecordCursor
        implements RecordCursor
{
    private final List<NomsType> nomsTypes;
    private final ResultSet rs;
    private Row currentRow;
    private long atLeastCount;
    private long count;

    public NomsRecordCursor(NomsSession nomsSession, List<NomsType> nomsTypes)
    {
        this.nomsTypes = nomsTypes;
        // TODO: execute ngql query
        rs = nomsSession.execute("");
        currentRow = null;
        atLeastCount = rs.getAvailableWithoutFetching();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!rs.isExhausted()) {
            currentRow = rs.one();
            count++;
            atLeastCount = count + rs.getAvailableWithoutFetching();
            return true;
        }
        return false;
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean getBoolean(int i)
    {
        return currentRow.getBool(i);
    }

    @Override
    public long getCompletedBytes()
    {
        return count;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public double getDouble(int i)
    {
        return currentRow.getDouble(i);
    }

    @Override
    public long getLong(int i)
    {
        throw new AssertionError("incomplete");
        /*
        switch (getNomsType(i)) {
            case INT:
                return currentRow.getInt(i);
            case BIGINT:
            case COUNTER:
                return currentRow.getLong(i);
            case TIMESTAMP:
                return currentRow.getTimestamp(i).getTime();
            case FLOAT:
                return floatToRawIntBits(currentRow.getFloat(i));
            default:
                throw new IllegalStateException("Cannot retrieve long for " + getNomsType(i));
        }
        */
    }

    private NomsType getNomsType(int i)
    {
        return nomsTypes.get(i);
    }

    @Override
    public Slice getSlice(int i)
    {
        NullableValue value = NomsType.getColumnValue(currentRow, i, nomsTypes.get(i));
        if (value.getValue() instanceof Slice) {
            return (Slice) value.getValue();
        }
        return utf8Slice(value.getValue().toString());
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTotalBytes()
    {
        return atLeastCount;
    }

    @Override
    public Type getType(int i)
    {
        return getNomsType(i).getNativeType();
    }

    @Override
    public boolean isNull(int i)
    {
        return currentRow.isNull(i);
    }
}
