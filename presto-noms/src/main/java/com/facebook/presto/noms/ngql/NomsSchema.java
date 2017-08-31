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

import com.facebook.presto.noms.NomsType;

import javax.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class NomsSchema
{
    private final JsonObject object;
    private final NgqlType rootType;
    private final Map<String, NgqlType> types = new HashMap<>();
    private final String primaryKey;

    public NomsSchema(NomsResult result)
    {
        object = result.value().asJsonObject();
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

    /*package*/ NgqlType resolve(NgqlType type)
    {
        return type.reference() ? types.get(type.name()) : type;
    }

    /*package*/ Map<String, NgqlType> types()
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
