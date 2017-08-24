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

import javax.json.JsonObject;

import java.util.HashMap;
import java.util.Map;

public class NgqlSchema
{
    private final JsonObject object;
    private final NgqlType rootType;
    private final Map<String, NgqlType> types = new HashMap<>();

    public NgqlSchema(NgqlResult result)
    {
        object = result.value().asJsonObject();
        for (JsonObject t : object.getJsonArray("types").getValuesAs(JsonObject.class)) {
            NgqlType type = new NgqlType(t);
            types.put(type.name(), type);
        }
        String rootTypeName = object.getJsonObject("queryType").getString("name");
        rootType = types.get(rootTypeName);
    }

    private NgqlType rootValueType()
    {
        return resolve(rootType.fieldType("root"));
    }

    public NgqlType lastCommitValueType()
    {
        return resolve(rootValueType().fieldType("value"));
    }

    public NgqlType resolve(NgqlType type)
    {
        return type.reference() ? types.get(type.name()) : type;
    }

    public Map<String, NgqlType> types()
    {
        return types;
    }
}
