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

import com.facebook.presto.spi.PrestoException;
import io.attic.presto.noms.NomsQuery;
import io.attic.presto.noms.NomsSchema;
import io.attic.presto.noms.NomsType;
import org.apache.commons.lang3.tuple.Pair;

import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

public class SchemaQuery
        extends NgqlQuery<SchemaQuery.Result>
{
    public static SchemaQuery create()
    {
        return new SchemaQuery();
    }

    protected String query()
    {
        return "query {\n" +
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
                "  }";
    }

    protected SchemaQuery.Result parseResult(JsonObject json)
    {
        return new SchemaQuery.Result(json);
    }

    @Override
    public String toString()
    {
        return query();
    }

    public static class Result
            implements NomsQuery.Result, NomsSchema
    {
        private final JsonObject object;
        private final NgqlType lastCommitValueType;
        private final Map<String, NgqlType> types = new HashMap<>();
        private final String primaryKey;
        private final TableStructure structure;
        private final boolean usesColumnRefs;

        private Result(JsonObject json)
        {
            JsonValue value;
            try {
                value = json.getValue("/data");
            }
            catch (JsonException e) {
                value = JsonValue.EMPTY_JSON_ARRAY;
            }
            object = value.asJsonObject();

            JsonObject schema = object.getJsonObject("__schema");
            for (JsonObject t : schema.getJsonArray("types").getValuesAs(JsonObject.class)) {
                NgqlType type = new NgqlType(t);
                types.put(type.name(), type);
            }
            String rootTypeName = schema.getJsonObject("queryType").getString("name");
            NgqlType rootType = types.get(rootTypeName);
            NgqlType rootValueType = resolve(rootType.fieldType("root"));
            lastCommitValueType = resolve(rootValueType.fieldType("value"));
            JsonObject meta = object.getValue("/root/meta").asJsonObject();
            primaryKey = meta.getString("primaryKey", null);

            switch (tableType().kind()) {
                case Struct:
                    structure = TableStructure.ColumnMajor;
                    usesColumnRefs = tableType().fields().values().iterator().next().kind() == NomsType.Kind.Ref;
                    break;
                case List:
                case Set:
                case Map:
                    structure = TableStructure.RowMajor;
                    usesColumnRefs = false;
                    break;
                default:
                    throw new PrestoException(NOT_SUPPORTED, "Unsupported table type: " + tableType());
            }
        }

        public TableStructure tableStructure()
        {
            return structure;
        }

        public boolean usesColumnRefs()
        {
            return usesColumnRefs;
        }

        public NomsType tableType()
        {
            return NgqlType.nomsType(lastCommitValueType, this);
        }

        public String primaryKey()
        {
            return primaryKey;
        }

        public List<Pair<String, NomsType>> columns()
        {
            NomsType tableType = tableType();
            Map<String, NomsType> fields;

            switch (tableStructure()) {
                case RowMajor:
                    fields = rowMajorFields(tableType);
                    break;
                case ColumnMajor:
                    // Column major. Columns are lists. Ignore other fields.
                    fields = tableType.fields().entrySet().stream().filter(e -> {
                        switch (e.getValue().kind()) {
                            case List: case Ref:
                                return true;
                            default:
                                return false;
                        }
                    }).collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));
                    break;
                default:
                    throw new AssertionError("unexpected table structure " + tableStructure());
            }

            return fields.entrySet().stream().map(e -> {
                switch (e.getValue().kind()) {
                    case List:
                        return Pair.of(e.getKey(), e.getValue().argument(0));
                    case Ref:
                        return Pair.of(e.getKey(), e.getValue().argument(0).argument(0));
                    default:
                        return Pair.of(e.getKey(), e.getValue());
                }
            }).collect(Collectors.toList());
        }

        private Map<String, NomsType> rowMajorFields(NomsType type)
        {
            switch (type.kind()) {
                case List:
                    return type.argument(0).fields();
                case Map:
                    return type.argument(1).fields();
                default:
                    throw new PrestoException(NOT_SUPPORTED,
                            "Unsupported table structure: " + type + ", must be List<Struct> | Map<Value, Struct>");
            }
        }

        public NgqlType resolve(NgqlType type)
        {
            return type.reference() ? types.get(type.name()) : type;
        }
    }
}
