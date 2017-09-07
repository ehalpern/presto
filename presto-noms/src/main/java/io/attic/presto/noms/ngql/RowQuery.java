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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.attic.presto.noms.NomsColumnHandle;
import io.attic.presto.noms.NomsTable;
import io.attic.presto.noms.NomsType;

import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;

public class RowQuery implements Ngql.Query<RowQuery.Result>
{
    private final String query;
    private final List<String> params;
    private final List<String> pathToTable;

    public RowQuery(
            NomsTable table,
            List<NomsColumnHandle> columns,
            TupleDomain<NomsColumnHandle> predicate,
            long offset,
            long limit)
    {
        List<String> path = pathToTable(table.tableType());
        this.params = paramsFromPrediate(table.schema(), columns, predicate, offset, limit);
        Map<String, NgqlType> fields = columns.stream().collect(Collectors.toMap(
                c -> c.getName(),
                c -> ngqlType(table, c)));
        this.query = "{\n" + buildQuery(path, params, fields, 1) + "}\n";
        this.pathToTable = path;
    }


    public String query()
    {
        return query;
    }

    public Class<RowQuery.Result> resultClass()
    {
        return RowQuery.Result.class;
    }

    public RowQuery.Result newResult(JsonObject json)
    {
        return new Result(json, pathToTable);
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
    private static List<String> paramsFromPrediate(NomsSchema schema, List<NomsColumnHandle> columns, TupleDomain<NomsColumnHandle> predicate, long offset, long limit)
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

    private static NgqlType ngqlType(NomsTable table, NomsColumnHandle column)
    {
        String name = column.getNomsType().name();
        if (name.equals("Number")) {
            name = "Float";
        }
        return verifyNotNull(table.schema().types().get(name), "NgqlType " + name + " not found");
    }

    private static List<String> pathToTable(NomsType tableType)
    {
        List<String> path = new ArrayList<>();
        path.add("root");
        path.add("value");
        switch (tableType.kind()) {
            case String:
            case Boolean:
            case Number:
            case Blob:
                break;
            case Set:
            case List:
            case Map:
                path.add("values");
                break;
            default:
                throw new IllegalStateException("Handling of type " + tableType.kind() + " is not implemented");
        }
        return path;
    }

    private static String buildQuery(List<String> path, List<String> params, Map<String, NgqlType> fields, int indent)
    {
        verify(path.size() > 0);
        String tabs = Strings.repeat("\t", indent);
        StringBuilder b = new StringBuilder(tabs + path.get(0));
        String nested;
        if (path.size() > 1) {
            nested = buildQuery(path.subList(1, path.size()), params, fields, indent + 1);
        }
        else {
            if (params.size() > 0) {
                b.append("(" + String.join(",", params) + ")");
            }
            nested = buildFieldQuery(fields, indent + 1);
        }
        if (nested.length() > 0) {
            b.append(" {\n" + nested + tabs + "}");
        }
        return b.append("\n").toString();
    }

    private static String buildFieldQuery(Map<String, NgqlType> fields, int indent)
    {
        String tabs = Strings.repeat("\t", indent);
        StringBuilder b = new StringBuilder();
        for (String field : fields.keySet()) {
            b.append(tabs + field);
            NgqlType type = fields.get(field);
            switch (type.kind()) {
                case SCALAR:
                case ENUM:
                    break;
                case OBJECT:
                    String nested = buildFieldQuery(type.fields(), indent + 1);
                    if (nested.length() > 0) {
                        b.append(" {\n" + nested + indent + "}");
                    }
                    break;
                case LIST:
                case NON_NULL:
                case UNION:
                    throw new AssertionError("kind " + type.kind() + " not implemented");
            }
            b.append("\n");
        }
        return b.toString();
    }

    @Override
    public String toString()
    {
        return query;
    }

    public static class Result
            implements Ngql.Result<RowQuery.Result>
    {
        private final JsonValue valueAtPath;

        private Result(JsonObject json, List<String> path)
        {
            String fullPath = "/data/" + String.join("/", path);
            JsonValue value;
            try {
                value = json.getValue(fullPath);
            }
            catch (JsonException e) {
                value = JsonValue.EMPTY_JSON_ARRAY;
            }
            this.valueAtPath = value;
        }

        public int size()
        {
            return valueAtPath.getValueType() == JsonValue.ValueType.ARRAY ?
                    valueAtPath.asJsonArray().size() : 1;
        }

        /*package*/ JsonValue value()
        {
            return valueAtPath;
        }

        /*package*/ Iterator<JsonValue> rows()
        {
            if (valueAtPath.getValueType() == JsonValue.ValueType.ARRAY) {
                return valueAtPath.asJsonArray().iterator();
            }
            else {
                return ImmutableList.of(valueAtPath).iterator();
            }
        }

        public String toString()
        {
            return valueAtPath.toString();
        }
    }
}

