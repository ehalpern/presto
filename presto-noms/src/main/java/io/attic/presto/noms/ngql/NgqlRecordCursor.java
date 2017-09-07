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
package io.attic.presto.noms.ngql;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.attic.presto.noms.NomsColumnHandle;
import io.attic.presto.noms.NomsSession;
import io.attic.presto.noms.NomsSplit;
import io.attic.presto.noms.NomsTable;
import io.attic.presto.noms.NomsType;

import javax.json.Json;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static io.airlift.slice.Slices.utf8Slice;

public class NgqlRecordCursor
        implements RecordCursor
{
    private final NomsSession session;
    private final NomsTable table;
    private final List<NomsColumnHandle> columns;
    private final RowQuery query;

    private Iterator<JsonValue> results = JsonValue.EMPTY_JSON_ARRAY.iterator();
    private JsonObject row = JsonValue.EMPTY_JSON_OBJECT;

    private long totalCount = 0;
    private long completedCount = 0;
    private int batchSize = 100;

    /*pacakge*/ NgqlRecordCursor(NomsSession session, NomsSplit split, NomsTable table, List<NomsColumnHandle> columns)
    {
        this.columns = verifyNotNull(columns, "columns is null");
        this.query = NgqlQuery.rowQuery(
                table, columns,
                split.getEffectivePredicate(),
                split.getOffset(),
                split.getLimit());
        this.table = table;
        this.session = session;
    }

    private Iterator<JsonValue> nextBatch()
    {
        // TODO: Implement batching. For now, read all rows on
        // the first batch and terminate the cursor on the second
        if (totalCount > 0) {
            return JsonValue.EMPTY_JSON_ARRAY.iterator();
        }
        else {
            RowQuery.Result result = session.execute(table.tableHandle().getTableName(), query);
            totalCount = result.size();
            return result.rows();
        }
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (results.hasNext()) {
            JsonValue rowValue = results.next();
            while (rowValue.equals(JsonValue.NULL) && results.hasNext()) {
                completedCount += 1;
                rowValue = results.next();
            }
            if (rowValue.equals(JsonValue.NULL)) {
                return false;
            }
            else if (rowValue.getValueType() != JsonValue.ValueType.OBJECT) {
                verify(columns.size() == 1, "expecting a single column");
                row = Json.createObjectBuilder().add(columns.get(0).getName(), rowValue).build();
            }
            else {
                row = rowValue.asJsonObject();
            }
            completedCount += 1;
            return true;
        }
        else {
            results = nextBatch();
            if (!results.hasNext()) {
                return false;
            }
            else {
                return advanceNextPosition();
            }
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completedCount;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public void close()
    {
    }

    private String nameOf(int i)
    {
        return columns.get(i).getName();
    }

    private NomsType typeOf(int i)
    {
        return columns.get(i).getNomsType();
    }

    private JsonValue valueOf(int i)
    {
        return row.get(nameOf(i));
    }

    @Override
    public boolean getBoolean(int i)
    {
        return Boolean.parseBoolean(valueOf(i).toString());
    }

    @Override
    public double getDouble(int i)
    {
        return ((JsonNumber) valueOf(i)).doubleValue();
    }

    @Override
    public long getLong(int i)
    {
        return ((JsonNumber) valueOf(i)).longValue();
    }

    @Override
    public Slice getSlice(int i)
    {
        NomsType nomsType = typeOf(i);
        Type nativeType = getType(i);
        NullableValue value;
        if (isNull(i)) {
            value = NullableValue.asNull(nativeType);
        }
        else {
            switch (nomsType.kind()) {
                case String:
                    String s = ((JsonString) valueOf(i)).getString();
                    value = NullableValue.of(nativeType, utf8Slice(s));
                    break;
                case Boolean:
                    value = NullableValue.of(nativeType, getBoolean(i));
                    break;
                case Number:
                    value = NullableValue.of(nativeType, getDouble(i));
                    break;
                case Blob:
                case Set:
                    value = NullableValue.of(nativeType, utf8Slice(buildSetValue(i, nomsType.arguments().get(0))));
                    break;
                case List:
                    value = NullableValue.of(nativeType, utf8Slice(buildListValue(i, nomsType.arguments().get(0))));
                    break;
                case Map:
                    value = NullableValue.of(nativeType, utf8Slice(buildMapValue(i, nomsType.arguments().get(0), nomsType.arguments().get(1))));
                    break;
                default:
                    throw new IllegalStateException("Handling of type " + nomsType + " is not implemented");
            }
        }
        if (value.getValue() instanceof Slice) {
            return (Slice) value.getValue();
        }
        return utf8Slice(value.getValue().toString());
    }

    private <T> List<T> getList(int i, Class<T> elementsClass)
    {
        throw new AssertionError("not implemented");
    }

    private <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass)
    {
        throw new AssertionError("not implemented");
    }

    private <T> Set<T> getSet(int i, Class<T> elementsClass)
    {
        throw new AssertionError("not implemented");
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTotalBytes()
    {
        return totalCount;
    }

    @Override
    public Type getType(int i)
    {
        return typeOf(i).nativeType();
    }

    @Override
    public boolean isNull(int i)
    {
        return valueOf(i) == JsonValue.NULL;
    }

    private String buildSetValue(int i, NomsType elemType)
    {
        return NomsType.buildArrayValue(getSet(i, elemType.javaType()), elemType);
    }

    private String buildListValue(int i, NomsType elemType)
    {
        return NomsType.buildArrayValue(getList(i, elemType.javaType()), elemType);
    }

    private String buildMapValue(int i, NomsType keyType, NomsType valueType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : getMap(i, keyType.javaType(), valueType.javaType()).entrySet()) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(NomsType.objectToString(entry.getKey(), keyType));
            sb.append(":");
            sb.append(NomsType.objectToString(entry.getValue(), valueType));
        }
        sb.append("}");
        return sb.toString();
    }
}
