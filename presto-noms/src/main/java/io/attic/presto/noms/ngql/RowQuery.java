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
import io.attic.presto.noms.NomsColumnHandle;
import io.attic.presto.noms.NomsQuery;
import io.attic.presto.noms.NomsSchema;

import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonValue;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class RowQuery
        extends NgqlQuery<RowQuery.Result>
{
    public static RowQuery create(
            NomsSchema schema,
            List<NomsColumnHandle> columns,
            TupleDomain<NomsColumnHandle> predicate,
            long offset,
            long limit)
    {
        return new RowQuery(schema, columns, predicate, offset, limit);
    }

    private final String query;
    private final List<String> params;

    private RowQuery(
            NomsSchema schema,
            List<NomsColumnHandle> columns,
            TupleDomain<NomsColumnHandle> predicate,
            long offset,
            long limit)
    {
        this.params = paramsFromPredicate(schema, columns, predicate, offset, limit);
        this.query = buildQuery(params, columns);
    }

    protected String query()
    {
        return query;
    }

    protected RowQuery.Result parseResult(JsonObject json)
    {
        return new Result(json);
    }

    private static String buildQuery(List<String> params, List<NomsColumnHandle> columns)
    {
        String paramList = params.isEmpty() ?
                "" : String.format("(%s)", String.join(",", params));
        List<String> fields = columns.stream().map(c -> c.getName()).collect(Collectors.toList());

        String query =
                "{ root { value {\n" +
                "  size\n" +
                "  values" + paramList + "{\n" +
                "    " + String.join("\n    ", fields) + "\n" +
                "  }\n" +
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
        private final JsonArray rows;
        private final int totalSize;

        private Result(JsonObject json)
        {
            String fullPath = "/data/root/value";
            JsonObject value;
            try {
                value = json.getValue(fullPath).asJsonObject();
            }
            catch (JsonException e) {
                value = JsonObject.EMPTY_JSON_OBJECT;
            }
            this.totalSize = value.getJsonNumber("size").intValue();
            this.rows = value.getJsonArray("values");
        }

        public int size()
        {
            return rows.size();
        }

        public Iterator<JsonValue> rows()
        {
            return rows.iterator();
        }

        public String toString()
        {
            return rows.toString();
        }
    }
}
