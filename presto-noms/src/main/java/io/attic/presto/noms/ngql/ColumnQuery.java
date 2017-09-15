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

import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import io.attic.presto.noms.NomsColumnHandle;
import io.attic.presto.noms.NomsQuery;
import io.attic.presto.noms.NomsSchema;

import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;

public class ColumnQuery
        extends NgqlQuery<ColumnQuery.Result>
{
    public static ColumnQuery create(
            NomsSchema schema,
            List<NomsColumnHandle> columns,
            TupleDomain<NomsColumnHandle> predicate,
            long offset,
            long limit)
    {
        return new ColumnQuery(schema, columns, predicate, offset, limit);
    }

    private final String query;
    private final List<String> params;

    private ColumnQuery(
            NomsSchema schema,
            List<NomsColumnHandle> columns,
            TupleDomain<NomsColumnHandle> predicate,
            long offset,
            long limit)
    {
        this.params = paramsFromPredicate(schema, columns, predicate, offset, limit);
        this.query = buildQuery(params, schema, columns);
    }

    protected String query()
    {
        return query;
    }

    protected ColumnQuery.Result parseResult(JsonObject json)
    {
        return new Result(json);
    }

    // {
    //   root {
    //     value {
    //       col0 { size, values(at:$offset, count: $count) }
    //       col1 { size, values(at:$offset, count: $count) }
    //       ...
    //       coln { size, values(at:$offset, count: $count) }
    //     }
    // }
    //
    private static String buildQuery(List<String> params, NomsSchema schema, List<NomsColumnHandle> columns)
    {
        List<String> fieldList;

        if (columns.isEmpty()) {
            verify(schema.columns().size() > 0, "schema must have at least one column");
            String dummyField = schema.columns().get(0).getKey();
            fieldList = ImmutableList.of(String.format("%s { size }", dummyField));
        }
        else {
            String paramList = params.isEmpty() ?
                    "" : String.format("(%s)", String.join(",", params));
            fieldList = columns.stream().map(
                    c -> String.format("%s { size, values%s }", c.getName(), paramList)
            ).collect(Collectors.toList());
        }
        String query =
                "{ root { value {\n" +
                "  " + String.join("\n  ", fieldList) + "\n" +
                "}}}";
        return query;
    }

    @Override
    public String toString()
    {
        return query;
    }

    public static class Result
            implements NomsQuery.Result
    {
        private final JsonObject columns;
        private final int size;

        private Result(JsonObject json)
        {
            String path = "/data/root/value";
            JsonValue value;
            try {
                value = json.getValue(path);
            }
            catch (JsonException e) {
                value = JsonValue.EMPTY_JSON_OBJECT;
            }
            verify(value.getValueType() == JsonValue.ValueType.OBJECT,
                    "commit value at %s in not an object");
            columns = value.asJsonObject();
            verify(columns.size() > 0, "response must contain at least 1 column");
            JsonObject firstColumn = columns.values().stream().findFirst().get().asJsonObject();
            if (firstColumn.containsKey("values")) {
                size = firstColumn.getJsonArray("values").size();
            }
            else {
                size = firstColumn.getInt("size", 0);
            }
        }

        public int size()
        {
            return size;
        }

        public String[] columnOfStrings(String column)
        {
            JsonArray array = columnArray(column);
            return array.stream().map(v -> ((JsonString) v).getString()).toArray(String[]::new);
        }

        public boolean[] columnOfBooleans(String column)
        {
            JsonArray array = columnArray(column);
            boolean[] result = new boolean[array.size()];
            int i = 0;
            for (JsonValue v : array) {
                result[i++] = v == JsonValue.TRUE;
            }
            return result;
        }

        public double[] columnOfDoubles(String column)
        {
            JsonArray array = columnArray(column);
            return array.stream().mapToDouble(v -> ((JsonNumber) v).doubleValue()).toArray();
        }

        public String toString()
        {
            return columns.toString();
        }

        private JsonArray columnArray(String name)
        {
            JsonObject o = verifyNotNull(columns.getJsonObject(name), "column %s no present", name);
            return o.getJsonArray("values");
        }
    }
}
