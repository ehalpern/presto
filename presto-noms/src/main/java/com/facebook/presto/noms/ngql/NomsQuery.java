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
package com.facebook.presto.noms.ngql;

import com.facebook.presto.noms.NomsColumnHandle;
import com.facebook.presto.noms.NomsTable;
import com.facebook.presto.noms.NomsType;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.base.Strings;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Form;
import org.apache.http.client.fluent.Request;

import javax.json.Json;
import javax.json.JsonReader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;

public class NomsQuery
{
    private final String query;
    private final List<String> pathToTable;

    private static final NomsQuery INTROSPECT_QUERY =
            new NomsQuery("query {\n" +
                    "    root {\n" +
                    "      meta {\n" +
                    "         primaryKey\n" +
                    "      }\n" +
                    "    }\n" +
                    "    __schema {\n" +
                    "      queryType { name }\n" +
                    "      mutationType { name }\n" +
                    "      types {\n" +
                    "        ...FullType\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "\n" +
                    "  fragment FullType on __Type {\n" +
                    "    kind\n" +
                    "    name\n" +
                    "    description\n" +
                    "    fields(includeDeprecated: true) {\n" +
                    "      name\n" +
                    "      description\n" +
                    "      args {\n" +
                    "        ...InputValue\n" +
                    "      }\n" +
                    "      type {\n" +
                    "        ...TypeRef\n" +
                    "      }\n" +
                    "      isDeprecated\n" +
                    "      deprecationReason\n" +
                    "    }\n" +
                    "    inputFields {\n" +
                    "      ...InputValue\n" +
                    "    }\n" +
                    "    interfaces {\n" +
                    "      ...TypeRef\n" +
                    "    }\n" +
                    "    enumValues(includeDeprecated: true) {\n" +
                    "      name\n" +
                    "      description\n" +
                    "      isDeprecated\n" +
                    "      deprecationReason\n" +
                    "    }\n" +
                    "    possibleTypes {\n" +
                    "      ...TypeRef\n" +
                    "    }\n" +
                    "  }\n" +
                    "\n" +
                    "  fragment InputValue on __InputValue {\n" +
                    "    name\n" +
                    "    description\n" +
                    "    type { ...TypeRef }\n" +
                    "    defaultValue\n" +
                    "  }\n" +
                    "\n" +
                    "  fragment TypeRef on __Type {\n" +
                    "    kind\n" +
                    "    name\n" +
                    "    ofType {\n" +
                    "      kind\n" +
                    "      name\n" +
                    "      ofType {\n" +
                    "        kind\n" +
                    "        name\n" +
                    "        ofType {\n" +
                    "          kind\n" +
                    "          name\n" +
                    "          ofType {\n" +
                    "            kind\n" +
                    "            name\n" +
                    "            ofType {\n" +
                    "              kind\n" +
                    "              name\n" +
                    "              ofType {\n" +
                    "                kind\n" +
                    "                name\n" +
                    "                ofType {\n" +
                    "                  kind\n" +
                    "                  name\n" +
                    "                }\n" +
                    "              }\n" +
                    "            }\n" +
                    "          }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }",
                    Collections.emptyList());

    private NomsQuery(String query, List<String> pathToTable)
    {
        this(query, pathToTable, Collections.emptyList());
    }

    private NomsQuery(String query, List<String> pathToTable, List<String> params)
    {
        this.query = query;
        this.pathToTable = pathToTable;
    }

    public static NomsQuery introspectQuery()
    {
        return INTROSPECT_QUERY;
    }

    public static NomsQuery rowQuery(
            NomsTable table,
            List<NomsColumnHandle> columns,
            TupleDomain<NomsColumnHandle> predicate,
            long offset,
            long limit)
    {
        // TODO: columns.isEmpty() should be interpreted as a count(*) query. How to represent this is result?
        List<String> path = pathToTable(table.tableType());
        List<String> params = paramsFromPrediate(table.schema(), columns, predicate, offset, limit);
        Map<String, NgqlType> fields = columns.stream().collect(Collectors.toMap(
                c -> c.getName(),
                c -> ngqlType(table, c)));
        String query = "{\n" + buildQuery(path, params, fields, 1) + "}\n";
        return new NomsQuery(query, path, params);
    }

    /**
     * Build parameter list from query predicate.
     *
     * Specifically, if there are constraints on the primary key column:
     * - If only exact key values are specified, use (keys: [values]) to select only those rows.
     * - If one or more non-exact key bounds (> or <) are specified, determine the range that spans
     *   the full set and use (key: start, through: end) to query that range. Note that the range
     *   query result will include rows corresponding to the low and high bounds if they exist, even
     *   if they are not required in the range. For example, a query for keys in the range 10 < k <= 100
     *   will include row 10 if it exists.
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

    public static NomsResult execute(URI nomsURI, String dataset, NomsQuery query)
            throws IOException
    {
        Content resp = Request.Post(nomsURI.toString() + "/graphql/").bodyForm(Form.form()
                .add("ds", dataset)
                .add("query", query.query)
                .build())
                .execute().returnContent();

        try (JsonReader reader = Json.createReader(resp.asStream())) {
            return new NomsResult(reader.readObject(), query.pathToTable);
        }
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
}
