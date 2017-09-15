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

import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import io.attic.presto.noms.NomsColumnHandle;

import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;

public class ColumnQuery
        extends NomsQuery<ColumnQuery.Result>
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
        Map<String, NgqlType> fields = columns.stream().collect(Collectors.toMap(
                c -> c.getName(),
                c -> ngqlType(schema, c)));
        this.query = buildQuery(params, fields);
    }

    protected String query()
    {
        return query;
    }

    protected ColumnQuery.Result parseResult(JsonObject json)
    {
        return new Result(json);
    }

    /**
     * Build parameter list from query predicate.
     * <p>
     * Specifically, if there are constraints on the primary key column:
     * - If only exact key values are specified, use (keys: [values]) to select only those rows.
     * - If one or more non-exact key bounds (> or <) are specified, determine the range that spans
     * the full set and use (key: start, through: end) to query that range. Note that the range
     * query result will include rows corresponding to the low and high bounds if they exist, even
     * if they are not required in the range. For example, a query for keys in the range 10 < k <= 100
     * will include row 10 if it exists.
     */
    private static List<String> paramsFromPredicate(NomsSchema schema, List<NomsColumnHandle> columns, TupleDomain<NomsColumnHandle> predicate, long offset, long limit)
    {
        List<String> params = new ArrayList<>();
        if (offset > 0) {
            params.add("at:" + offset);
        }
        if (limit > 0) {
            params.add("count:" + limit);
        }
        if (predicate.isAll() || schema.primaryKey() == null) {
            return params;
        }
        else {
            String pk = schema.primaryKey();
            return columns.stream().filter(c -> c.getName().equals(pk)).map(c -> {
                Domain constraints = predicate.getDomains().get().get(c);
                return exactValues(constraints).map(values -> {
                    params.add("keys: " + values);
                    return params;
                }).orElseGet(() ->
                        keyBounds(constraints).map(bounds -> {
                            if (bounds.getLow().getValueBlock().isPresent()) {
                                params.add("key: " + bounds.getLow().getValue());
                            }
                            if (bounds.getHigh().getValueBlock().isPresent()) {
                                params.add("through: " + bounds.getHigh().getValue());
                            }
                            return params;
                        }).get());
            }).findFirst().get();
        }
    }

    private static Optional<List<Object>> exactValues(Domain domain)
    {
        List<Object> values = new ArrayList<>();
        for (Range r : domain.getValues().getRanges().getOrderedRanges()) {
            if (r.getLow().getBound() != Marker.Bound.EXACTLY) {
                return Optional.empty();
            }
            values.add(r.getLow().getValue());
        }
        return Optional.of(values);
    }

    private static Optional<Range> keyBounds(Domain domain)
    {
        Range range = null;
        for (Range r : domain.getValues().getRanges().getOrderedRanges()) {
            if (range == null) {
                range = r;
            }
            else {
                range = range.span(r);
            }
        }
        return Optional.ofNullable(range);
    }

    private static NgqlType ngqlType(NomsSchema schema, NomsColumnHandle column)
    {
        String name = column.getNomsType().name();
        if (name.equals("Number")) {
            name = "Float";
        }
        return verifyNotNull(schema.types().get(name), "NgqlType " + name + " not found");
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
    private static String buildQuery(List<String> params, Map<String, NgqlType> fields)
    {
        String paramList = params.isEmpty() ?
                "" : String.format("(%s)", String.join(",", params));
        List<String> fieldList = fields.keySet().stream().map(
                f -> String.format("%s { size, values%s }", f, paramList)
        ).collect(Collectors.toList());

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
        private final long totalSize;
        private int size;

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
            this.columns = value.asJsonObject();
            JsonObject firstColumn = this.columns.values().stream().findFirst().orElse(JsonValue.EMPTY_JSON_OBJECT).asJsonObject();
            totalSize = firstColumn.getInt("size", 0);
            size = firstColumn.getJsonArray("values").size();
        }

        public int size()
        {
            return size;
        }

        public long totalSize()
        {
            return totalSize;
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
            JsonArray array = o.getJsonArray("values");
            // lazily initialize size;
            verify(size == 0 || size == array.size());
            size = array.size();
            return array;
        }
    }
}
