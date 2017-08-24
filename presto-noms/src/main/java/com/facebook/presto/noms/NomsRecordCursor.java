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

import com.facebook.presto.noms.util.NgqlQuery;
import com.facebook.presto.noms.util.NgqlResult;
import com.facebook.presto.noms.util.NgqlType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Verify;
import io.airlift.slice.Slice;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class NomsRecordCursor
        implements RecordCursor
{
    private final NomsSession session;
    private final NomsTable table;
    private final List<NomsColumnHandle> columns;
    private final List<String> path;
    private final NgqlQuery query;

    private Iterator<JsonValue> results = JsonValue.EMPTY_JSON_ARRAY.iterator();
    private JsonObject row = JsonValue.EMPTY_JSON_OBJECT;

    private long totalCount = 0;
    private long completedCount = 0;
    private int batchSize = 100;

    public NomsRecordCursor(NomsSession session, NomsSplit nomsSplit, NomsTable table, List<NomsColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");

        Map<String, NgqlType> fields = columns.stream().collect(Collectors.toMap(
                c -> c.getName(),
                c -> ngqlType(table, c)));
        this.path = NomsType.pathToTable(table.getTableType());
        this.columns = columns;
        this.query = NgqlQuery.tableQuery(path, fields);
        this.table = table;
        this.session = session;
    }

    private static NgqlType ngqlType(NomsTable table, NomsColumnHandle column)
    {
        String name = column.getNomsType().getName();
        if (name.equals("Number")) {
            name = "Float";
        }
        return requireNonNull(table.getSchema().types().get(name), "NgqlType " + name + " not found");
    }

    private JsonArray queryTable(long offset, long limit)
            throws IOException
    {
        NgqlResult result = session.execute(
                table.getTableHandle().getTableName(),
                query);
        JsonValue tableValue = result.json().getValue("/data/" + String.join("/", path));
        if (tableValue.getValueType() == JsonValue.ValueType.ARRAY) {
            return tableValue.asJsonArray();
        }
        else {
            return Json.createArrayBuilder().add(tableValue).build();
        }
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (results.hasNext()) {
            JsonValue rowValue = results.next();
            if (rowValue.getValueType() != JsonValue.ValueType.OBJECT) {
                Verify.verify(columns.size() == 1, "expecting a single column");
                row = Json.createObjectBuilder().add(columns.get(0).getName(), rowValue).build();
            }
            else {
                row = rowValue.asJsonObject();
            }
            completedCount += 1;
            return true;
        }
        else if (totalCount % batchSize != 0) {
            return false;
        }
        else {
            try {
                JsonArray next = queryTable(totalCount, batchSize);
                totalCount += next.size();
                results = next.iterator();
                if (!results.hasNext()) {
                    return false;
                }
                else {
                    return advanceNextPosition();
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close()
    {
    }

    private JsonValue columnValue(int i)
    {
        String field = columns.get(i).getName();
        return row.get(field);
    }

    @Override
    public boolean getBoolean(int i)
    {
        return Boolean.parseBoolean(columnValue(i).toString());
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
    public double getDouble(int i)
    {
        return Double.parseDouble(columnValue(i).toString());
    }

    @Override
    public long getLong(int i)
    {
        return Long.parseLong(columnValue(i).toString());
    }

    private NomsType getNomsType(int i)
    {
        return columns.get(i).getNomsType();
    }

    @Override
    public Slice getSlice(int i)
    {
        NomsType nomsType = columns.get(i).getNomsType();
        Type nativeType = nomsType.getNativeType();
        NullableValue value;
        if (isNull(i)) {
            value = NullableValue.asNull(nativeType);
        }
        else {
            switch (nomsType.getKind()) {
                case String:
                    String s = columnValue(i).toString();
                    // Strip quotes
                    s = s.substring(1, s.length() - 1);
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
                    NomsType.checkTypeArguments(nomsType, 1, nomsType.getArguments());
                    value = NullableValue.of(nativeType, utf8Slice(buildSetValue(i, nomsType.getArguments().get(0))));
                    break;
                case List:
                    NomsType.checkTypeArguments(nomsType, 1, nomsType.getArguments());
                    value = NullableValue.of(nativeType, utf8Slice(buildListValue(i, nomsType.getArguments().get(0))));
                    break;
                case Map:
                    NomsType.checkTypeArguments(nomsType, 2, nomsType.getArguments());
                    value = NullableValue.of(nativeType, utf8Slice(buildMapValue(i, nomsType.getArguments().get(0), nomsType.getArguments().get(1))));
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

    private <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass)
    {
        throw new AssertionError("not implemented");
    }

    private <T> Set<T> getSet(int i, Class<T> elementsClass)
    {
        throw new AssertionError("not implemented");
    }

    private <T> List<T> getList(int i, Class<T> elementsClass)
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
        return getNomsType(i).getNativeType();
    }

    @Override
    public boolean isNull(int i)
    {
        return columnValue(i) == JsonValue.NULL;
    }

    private String buildSetValue(int i, NomsType elemType)
    {
        return NomsType.buildArrayValue(getSet(i, elemType.getJavaType()), elemType);
    }

    private String buildListValue(int i, NomsType elemType)
    {
        return NomsType.buildArrayValue(getList(i, elemType.getJavaType()), elemType);
    }

    private String buildMapValue(int i, NomsType keyType, NomsType valueType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : getMap(i, keyType.getJavaType(), valueType.getJavaType()).entrySet()) {
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
