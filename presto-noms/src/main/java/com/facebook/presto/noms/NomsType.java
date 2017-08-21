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

import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.facebook.presto.noms.util.CassandraCqlUtils;
import com.facebook.presto.noms.util.NgqlSchema;
import com.facebook.presto.noms.util.NgqlType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.airlift.slice.Slices.utf8Slice;

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
                return new DerivedNomsType(RootNomsType.LIST, ImmutableList.of(
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
                    return new DerivedNomsType(RootNomsType.LIST, ImmutableList.of(
                            NomsType.from(ngqlType.fieldType("values"), schema)));
                }
                if (mapPattern.matcher(typeName).matches()) {
                    return new DerivedNomsType(RootNomsType.MAP, ImmutableList.of(
                            NomsType.from(ngqlType.fieldType("keys"), schema),
                            NomsType.from(ngqlType.fieldType("values"), schema)));
                }
                if (refPattern.matcher(typeName).matches()) {
                    return new DerivedNomsType(RootNomsType.MAP, ImmutableList.of(
                            NomsType.from(ngqlType.fieldType("targetValue"), schema)));
                }
                if (setPattern.matcher(typeName).matches()) {
                    return new DerivedNomsType(RootNomsType.SET, ImmutableList.of(
                            NomsType.from(ngqlType.fieldType("values"), schema)));
                }
                if (structPattern.matcher(typeName).matches()) {
                    return new DerivedNomsType(RootNomsType.STRUCT, Collections.EMPTY_LIST,
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
                        return RootNomsType.NUMBER;
                    case "Float":
                        return RootNomsType.NUMBER;
                    case "String":
                        return RootNomsType.STRING;
                    default:
                        throw new PrestoException(NOT_SUPPORTED, "unsupported SCALAR name: " + ngqlType.name());
                }
            case ENUM:
            case UNION:
            default:
                throw new PrestoException(NOT_SUPPORTED, "unsupported kind: " + ngqlType.name());
        }
    }

    boolean typeOf(RootNomsType... types);

    RootNomsType getRootNomsType();

    Type getNativeType();

    Class<?> getJavaType();

    List<NomsType> getTypeArguments();

    Map<String, NomsType> getFields();

    public static NullableValue getColumnValue(Row row, int i, NomsType nomsType)
    {
        return getColumnValue(row, i, nomsType, nomsType.getTypeArguments());
    }

    public static NullableValue getColumnValue(Row row, int i, NomsType nomsType,
            List<NomsType> typeArguments)
    {
        Type nativeType = nomsType.getNativeType();
        if (row.isNull(i)) {
            return NullableValue.asNull(nativeType);
        }
        switch (nomsType.getRootNomsType()) {
            case STRING:
                return NullableValue.of(nativeType, utf8Slice(row.getString(i)));
            case BOOLEAN:
                return NullableValue.of(nativeType, row.getBool(i));
            case NUMBER:
                return NullableValue.of(nativeType, row.getDouble(i));
            case BLOB:
            case SET:
                checkTypeArguments(nomsType, 1, typeArguments);
                return NullableValue.of(nativeType, utf8Slice(buildSetValue(row, i, typeArguments.get(0))));
            case LIST:
                checkTypeArguments(nomsType, 1, typeArguments);
                return NullableValue.of(nativeType, utf8Slice(buildListValue(row, i, typeArguments.get(0))));
            case MAP:
                checkTypeArguments(nomsType, 2, typeArguments);
                return NullableValue.of(nativeType, utf8Slice(buildMapValue(row, i, typeArguments.get(0), typeArguments.get(1))));
            default:
                throw new IllegalStateException("Handling of type " + nomsType + " is not implemented");
        }
    }

    static String buildSetValue(Row row, int i, NomsType elemType)
    {
        return buildArrayValue(row.getSet(i, elemType.getJavaType()), elemType);
    }

    static String buildListValue(Row row, int i, NomsType elemType)
    {
        return buildArrayValue(row.getList(i, elemType.getJavaType()), elemType);
    }

    static String buildMapValue(Row row, int i, NomsType keyType, NomsType valueType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : row.getMap(i, keyType.getJavaType(), valueType.getJavaType()).entrySet()) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append(objectToString(entry.getKey(), keyType));
            sb.append(":");
            sb.append(objectToString(entry.getValue(), valueType));
        }
        sb.append("}");
        return sb.toString();
    }

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
        switch (elemType.getRootNomsType()) {
            case STRING:
                return CassandraCqlUtils.quoteStringLiteralForJson(object.toString());
            case BLOB:
                return CassandraCqlUtils.quoteStringLiteralForJson(Bytes.toHexString((ByteBuffer) object));
            case BOOLEAN:
            case NUMBER:
                return object.toString();
            default:
                throw new IllegalStateException("Handling of type " + elemType + " is not implemented");
        }
    }
}
