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
package com.facebook.presto.noms;

import com.datastax.driver.core.utils.Bytes;
import com.facebook.presto.noms.util.CassandraCqlUtils;
import com.facebook.presto.noms.util.NgqlSchema;
import com.facebook.presto.noms.util.NgqlType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

@JsonDeserialize(using = NomsTypeDeserializer.class)
public interface NomsType
{
    static NomsType from(NgqlType ngqlType, NgqlSchema schema)
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
                return new DerivedNomsType(ngqlType.name(), RootNomsType.List, ImmutableList.of(
                        NomsType.from(ngqlType.ofType(), schema)));
            case OBJECT:
                String typeName = ngqlType.name();
                if (emptyPattern.matcher(typeName).matches()) {
                    switch (typeName) {
                        case "EmptyList":
                            return DerivedNomsType.EMPTY_LIST;
                        case "EmptyMap":
                            return DerivedNomsType.EMPTY_MAP;
                        case "EmptySet":
                            return DerivedNomsType.EMPTY_SET;
                        default:
                            throw new AssertionError("unexpected " + typeName);
                    }
                }
                if (listPattern.matcher(typeName).matches()) {
                    return new DerivedNomsType(typeName, RootNomsType.List, ImmutableList.of(
                            NomsType.from(ngqlType.fieldType("values"), schema)));
                }
                if (mapPattern.matcher(typeName).matches()) {
                    return new DerivedNomsType(typeName, RootNomsType.Map, ImmutableList.of(
                            NomsType.from(ngqlType.fieldType("keys"), schema),
                            NomsType.from(ngqlType.fieldType("values"), schema)));
                }
                if (refPattern.matcher(typeName).matches()) {
                    return new DerivedNomsType(typeName, RootNomsType.Map, ImmutableList.of(
                            NomsType.from(ngqlType.fieldType("targetValue"), schema)));
                }
                if (setPattern.matcher(typeName).matches()) {
                    return new DerivedNomsType(typeName, RootNomsType.Set, ImmutableList.of(
                            NomsType.from(ngqlType.fieldType("values"), schema)));
                }
                if (structPattern.matcher(typeName).matches()) {
                    return new DerivedNomsType(typeName, RootNomsType.Struct, Collections.EMPTY_LIST,
                            ngqlType.fields().entrySet().stream().collect(Collectors.toMap(
                                    e -> e.getKey(),
                                    e -> NomsType.from(e.getValue(), schema))));
                }
                if (typePattern.matcher(typeName).matches()) {
                    throw new AssertionError("Not implelented");
                }
            case SCALAR:
                switch (ngqlType.name()) {
                    case "Boolean":
                        return RootNomsType.Boolean;
                    case "Float":
                        return RootNomsType.Number;
                    case "String":
                        return RootNomsType.String;
                    default:
                        throw new PrestoException(NOT_SUPPORTED, "unsupported SCALAR name: " + ngqlType.name());
                }
            case ENUM:
            case UNION:
            default:
                throw new PrestoException(NOT_SUPPORTED, "unsupported kind: " + ngqlType.name());
        }
    }

    static List<String> pathToTable(NomsType tableType)
    {
        List<String> path = new ArrayList<>();
        path.add("root");
        path.add("value");
        switch (tableType.getRootType()) {
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
                throw new IllegalStateException("Handling of type " + tableType.getRootType() + " is not implemented");
        }
        return path;
    }

    boolean typeOf(RootNomsType... types);

    String getName();

    RootNomsType getRootType();

    Type getNativeType();

    Class<?> getJavaType();

    List<NomsType> getArguments();

    Map<String, NomsType> getFields();

    @VisibleForTesting
    static String buildArrayValue(Collection<?> collection, NomsType elemType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Object value : collection) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(value, elemType));
        }
        sb.append("]");
        return sb.toString();
    }

    static void checkTypeArguments(NomsType type, int expectedSize,
            List<NomsType> typeArguments)
    {
        if (typeArguments == null || typeArguments.size() != expectedSize) {
            throw new IllegalArgumentException("Wrong number of type arguments " + typeArguments
                    + " for " + type);
        }
    }

    static String objectToString(Object object, NomsType elemType)
    {
        switch (elemType.getRootType()) {
            case String:
                return CassandraCqlUtils.quoteStringLiteralForJson(object.toString());
            case Blob:
                return CassandraCqlUtils.quoteStringLiteralForJson(Bytes.toHexString((ByteBuffer) object));
            case Boolean:
            case Number:
                return object.toString();
            default:
                throw new IllegalStateException("Handling of type " + elemType + " is not implemented");
        }
    }
}
