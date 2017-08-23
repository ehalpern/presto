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
package com.facebook.presto.noms.util;

import com.google.common.base.Strings;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Form;
import org.apache.http.client.fluent.Request;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;

public class NgqlUtil
{
    private NgqlUtil()
    {
    }

    private static final String INTROSPECT_QUERY =
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

    public static JsonObject executeQuery(URI nomsURI, String dataset, String query)
            throws IOException
    {
        Content resp = Request.Post(nomsURI.toString() + "/graphql/").bodyForm(Form.form()
                .add("ds", dataset)
                .add("query", query)
                .build())
                .execute().returnContent();

        try (JsonReader reader = Json.createReader(resp.asStream())) {
            return reader.readObject();
        }
    }

    public static NgqlSchema introspectQuery(URI nomsURI, String dataset)
            throws IOException
    {
        return new NgqlSchema(executeQuery(nomsURI, dataset, INTROSPECT_QUERY));
    }

    public static String buildTableQuery(List<String> rootPath, Map<String, NgqlType> fields)
    {
        return "{\n" + buildQuery(rootPath, fields, 1) + "}\n";
    }

    private static String buildQuery(List<String> path, Map<String, NgqlType> fields, int indent)
    {
        verify(path.size() > 0);
        String tabs = Strings.repeat("\t", indent);
        StringBuilder b = new StringBuilder(tabs + path.get(0));
        String nested;
        if (path.size() > 1) {
            nested = buildQuery(path.subList(1, path.size()), fields, indent + 1);
        }
        else {
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
}
