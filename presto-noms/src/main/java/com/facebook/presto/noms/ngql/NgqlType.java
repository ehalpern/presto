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
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;

import javax.json.JsonObject;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

/*package*/ class NgqlType
{
    static NomsType nomsType(NgqlType ngqlType, NomsSchema schema)
    {
        final Pattern emptyPattern = Pattern.compile("Empty(List|Map|Set)");
        final Pattern listPattern = Pattern.compile("(.+)List");
        final Pattern mapPattern = Pattern.compile("(.+)To(.+)Map");
        final Pattern refPattern = Pattern.compile("(.+)Ref");
        final Pattern setPattern = Pattern.compile("(.+)Set");
        final Pattern structPattern = Pattern.compile("([^_]+)_.+");
        final Pattern typePattern = Pattern.compile("Type_(.+)");

        ngqlType = schema.resolve(ngqlType);
        switch (ngqlType.kind()) {
            case LIST:
                return new NomsType(ngqlType.name(), NomsType.Kind.List, ImmutableList.of(
                        nomsType(ngqlType.ofType(), schema)));
            case OBJECT:
                String typeName = ngqlType.name();
                if (emptyPattern.matcher(typeName).matches()) {
                    switch (typeName) {
                        case "EmptyList":
                            return NomsType.EMPTY_LIST;
                        case "EmptyMap":
                            return NomsType.EMPTY_MAP;
                        case "EmptySet":
                            return NomsType.EMPTY_SET;
                        default:
                            throw new AssertionError("unexpected " + typeName);
                    }
                }
                if (listPattern.matcher(typeName).matches()) {
                    return new NomsType(typeName, NomsType.Kind.List, ImmutableList.of(
                            nomsType(ngqlType.fieldType("values"), schema)));
                }
                if (mapPattern.matcher(typeName).matches()) {
                    return new NomsType(typeName, NomsType.Kind.Map, ImmutableList.of(
                            nomsType(ngqlType.fieldType("keys"), schema),
                            nomsType(ngqlType.fieldType("values"), schema)));
                }
                if (refPattern.matcher(typeName).matches()) {
                    return new NomsType(typeName, NomsType.Kind.Map, ImmutableList.of(
                            nomsType(ngqlType.fieldType("targetValue"), schema)));
                }
                if (setPattern.matcher(typeName).matches()) {
                    return new NomsType(typeName, NomsType.Kind.Set, ImmutableList.of(
                            nomsType(ngqlType.fieldType("values"), schema)));
                }
                if (structPattern.matcher(typeName).matches()) {
                    return new NomsType(typeName, NomsType.Kind.Struct, Collections.EMPTY_LIST,
                            ngqlType.fields().entrySet().stream().collect(Collectors.toMap(
                                    e -> e.getKey(),
                                    e -> nomsType(e.getValue(), schema))));
                }
                if (typePattern.matcher(typeName).matches()) {
                    throw new AssertionError("Not implelented");
                }
            case SCALAR:
                switch (ngqlType.name()) {
                    case "Boolean":
                        return NomsType.BOOLEAN;
                    case "Float":
                        return NomsType.NUMBER;
                    case "String":
                        return NomsType.STRING;
                    default:
                        throw new PrestoException(NOT_SUPPORTED, "unsupported SCALAR name: " + ngqlType.name());
                }
            case ENUM:
            case UNION:
            default:
                throw new PrestoException(NOT_SUPPORTED, "unsupported kind: " + ngqlType.name());
        }
    }

    enum Kind
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
