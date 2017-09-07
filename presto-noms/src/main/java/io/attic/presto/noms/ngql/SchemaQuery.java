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

import io.attic.presto.noms.NomsType;

import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonValue;

import java.util.HashMap;
import java.util.Map;

public class SchemaQuery implements NgqlQuery<SchemaQuery.Result>
{
    public String query()
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

    public Class<SchemaQuery.Result> resultClass()
    {
        return SchemaQuery.Result.class;
    }

    public SchemaQuery.Result newResult(JsonObject json)
    {
        return new SchemaQuery.Result(json);
    }

    @Override
    public String toString()
    {
        return query();
    }

    public static class Result implements NgqlQuery.Result, NomsSchema
    {
        private final JsonObject object;
        private final NgqlType rootType;
        private final Map<String, NgqlType> types = new HashMap<>();
        private final String primaryKey;

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
            rootType = types.get(rootTypeName);
            JsonObject meta = object.getValue("/root/meta").asJsonObject();
            primaryKey = meta.getString("primaryKey", null);
        }

        public NomsType tableType()
        {
            return NgqlType.nomsType(lastCommitValueType(), this);
        }

        public String primaryKey()
        {
            return primaryKey;
        }

        public NgqlType resolve(NgqlType type)
        {
            return type.reference() ? types.get(type.name()) : type;
        }

        public Map<String, NgqlType> types()
        {
            return types;
        }

        private NgqlType rootValueType()
        {
            return resolve(rootType.fieldType("root"));
        }

        private NgqlType lastCommitValueType()
        {
            return resolve(rootValueType().fieldType("value"));
        }
    }
}
