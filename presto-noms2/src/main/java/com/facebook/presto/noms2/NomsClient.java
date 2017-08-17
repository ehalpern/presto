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
package com.facebook.presto.noms2;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Form;
import org.apache.http.client.fluent.Request;

import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class NomsClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, Map<String, NomsTable>>> schemas;

    @Inject
    public NomsClient(NomsConfig config, JsonCodec<Map<String, List<NomsTable>>> catalogCodec)
            throws IOException
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config));
    }

    public Set<String> getSchemaNames()
    {
        // Schema -> noms2 database
        // Return the db specified in the configuration.
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        // Set<Table> -> Set<NomsDataset>
        // Ensure schema name matches db name in config
        // Return all dataset names
        // Should this be determine at creation time?
        requireNonNull(schema, "schema is null");
        Map<String, NomsTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public NomsTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, NomsTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Supplier<Map<String, Map<String, NomsTable>>> schemasSupplier(final JsonCodec<Map<String, List<NomsTable>>> catalogCodec, final NomsConfig config)
    {
        return () -> {
            try {
                return lookupSchemas(config, catalogCodec);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        };
    }

    private static Map<String, Map<String, NomsTable>> lookupSchemas(NomsConfig config, JsonCodec<Map<String, List<NomsTable>>> catalogCodec)
            throws IOException
    {
        String introspectQuery =
                "query IntrospectionQuery {\n" +
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
                "  }";

        Content resp = Request.Post(config.getNgqlURI()).bodyForm(Form.form()
                .add("ds",  config.getNomsDS())
                .add("query",  introspectQuery)
                .build())
                .execute().returnContent();


        try (JsonReader reader = Json.createReader(resp.asStream())) {
            JsonObject r = reader.readObject();
            if (r != null) {

            }
        }

        URI metadataUri = config.getMetadata();
        URL result = metadataUri.toURL();
        String json = Resources.toString(result, UTF_8);
        Map<String, List<NomsTable>> catalog = catalogCodec.fromJson(json);

        return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
    }

    private static Function<List<NomsTable>, Map<String, NomsTable>> resolveAndIndexTables(final URI metadataUri)
    {
        return tables -> {
            Iterable<NomsTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
            return ImmutableMap.copyOf(uniqueIndex(resolvedTables, NomsTable::getName));
        };
    }

    private static Function<NomsTable, NomsTable> tableUriResolver(final URI baseUri)
    {
        return table -> {
            List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
            return new NomsTable(table.getName(), table.getColumns(), sources);
        };
    }

    private static Function<NomsTable, NomsTable> nomsTableFromNgqlSchema(JsonObject json)
    {
        return table -> {
            JsonObject schema = json.getJsonObject("/data/__schema");
            String rootType = schema.getJsonString("/queryType/name").getString();
            JsonArray types = schema.getJsonArray("/types");


            return new NomsTable(table.getName(), table.getColumns(), null);
        };
    }
}
