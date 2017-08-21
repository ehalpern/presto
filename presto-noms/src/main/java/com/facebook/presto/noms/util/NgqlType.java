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

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class NgqlType
{
    public enum Kind
    {
        ENUM,
        LIST,
        NON_NULL,
        OBJECT,
        SCALAR,
        UNION
    }

    private final boolean nonNull;
    private final String name;
    private final Kind kind;
    private final NgqlType ofType;
    private final Map<String, NgqlType> fields;
    private final boolean reference;

    /*package*/ NgqlType(JsonObject o)
    {
        // if nonNull, note it and reveal the underlying type
        nonNull = o.getString("kind").equals(Kind.NON_NULL.toString());
        o = nonNull ? o.getJsonObject("ofType") : o;

        name = o.getString("name", "");
        kind = Kind.valueOf(o.getString("kind"));
        if (!o.containsKey("ofType") || o.isNull("ofType")) {
            ofType = null;
        }
        else {
            ofType = new NgqlType(o.getJsonObject("ofType"));
        }

        // if this is an OBJECT and there's no fields entry, it's a reference
        reference = !o.containsKey("fields") && kind == Kind.OBJECT;

        if (!o.containsKey("fields") || o.isNull("fields")) {
            this.fields = Collections.emptyMap();
        }
        else {
            this.fields = o.getJsonArray("fields").stream().collect(
                    Collectors.toMap(
                            v -> v.asJsonObject().getString("name"),
                            v -> new NgqlType(v.asJsonObject().getJsonObject("type"))));
        }
    }

    public boolean reference()
    {
        return reference;
    }

    public String name()
    {
        return name;
    }

    public Kind kind()
    {
        return kind;
    }

    public Map<String, NgqlType> fields()
    {
        return fields;
    }

    public NgqlType ofType()
    {
        return ofType;
    }

    public boolean nonNull()
    {
        return nonNull;
    }

    public NgqlType fieldType(String field)
    {
        return fields.get(field);
    }
}
