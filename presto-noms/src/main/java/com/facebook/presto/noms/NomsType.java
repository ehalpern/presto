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
import com.facebook.presto.noms.util.NgqlType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static io.airlift.slice.Slices.utf8Slice;

public class NomsType implements Comparable<NomsType>
{
    public static final NomsType BLOB = new NomsType(VarbinaryType.VARBINARY, ByteBuffer.class);
    public static final NomsType CYCLE = new NomsType(VarbinaryType.VARBINARY, null); // TODO: verify native type
    public static final NomsType BOOLEAN = new NomsType(BooleanType.BOOLEAN, Boolean.class);
    public static final NomsType NUMBER = new NomsType(DoubleType.DOUBLE, Double.class);
    public static final NomsType STRING = new NomsType(VarcharType.VARCHAR, String.class);
    public static final NomsType LIST = new NomsType(VarcharType.VARCHAR, null);    // TODO: Why not List?
    public static final NomsType MAP = new NomsType(VarcharType.VARCHAR, null);     // TODO: Why not Map?
    public static final NomsType REF = new NomsType(VarcharType.VARCHAR, null);     // TODO: Verify native type
    public static final NomsType SET = new NomsType(VarcharType.VARCHAR, null);     // TODO: Why not Set?
    public static final NomsType STRUCT = new NomsType(VarcharType.VARCHAR, null);  // TODO: Why not Row?
    public static final NomsType TYPE = new NomsType(VarcharType.VARCHAR, null);    // TODO: Verify native type
    public static final NomsType UNION = new NomsType(VarcharType.VARCHAR, null);   // TODO: determine native type

    private static final Pattern LIST_PATTERN = Pattern.compile("(.+)List");
    private static final Pattern MAP_PATTERN = Pattern.compile("(.+)To(.+)Map");
    private static final Pattern REF_PATTERN = Pattern.compile("(.+)Ref");
    private static final Pattern SET_PATTERN = Pattern.compile("(.+)Set");
    private static final Pattern STRUCT_PATTERN = Pattern.compile("([^_]+)_.+");
    private static final Pattern TYPE_PATTERN = Pattern.compile("Type_(.+)");

    public static NomsType from(NgqlType ngqlType) {
            switch (ngqlType.kind()) {
            case LIST:
                return new NomsType(LIST, ImmutableList.of(NomsType.from(ngqlType.ofType())));
            case OBJECT:
                String typeName = ngqlType.name();
                if (LIST_PATTERN.matcher(typeName).matches()) {
                    return new NomsType(LIST, ImmutableList.of(
                            // TODO: ofType?
                            NomsType.from(ngqlType.fields().get("values")))
                    );
                }
                if (MAP_PATTERN.matcher(typeName).matches()) {
                    return new NomsType(MAP, ImmutableList.of(
                            // TODO: ofType?
                            NomsType.from(ngqlType.fields().get("keys")),
                            NomsType.from(ngqlType.fields().get("values"))
                    ));
                }
                if (REF_PATTERN.matcher(typeName).matches()) {
                    return new NomsType(MAP, ImmutableList.of(
                            NomsType.from(ngqlType.fields().get("targetValue"))
                    ));
                }
                if (SET_PATTERN.matcher(typeName).matches()) {
                    return new NomsType(SET, ImmutableList.of(
                            // TODO: ofType?
                            NomsType.from(ngqlType.fields().get("values"))
                    ));
                }
                if (STRUCT_PATTERN.matcher(typeName).matches()) {
                    return new NomsType(STRUCT, Collections.EMPTY_LIST,
                            ngqlType.fields().entrySet().stream().collect(Collectors.toMap(
                                e -> e.getKey(),
                                e -> NomsType.from(e.getValue())
                            ))
                    );
                }
                if (TYPE_PATTERN.matcher(typeName).matches()) {
                    throw new AssertionError("Not implelented");
                }
            case SCALAR:
                switch (ngqlType.name()) {
                    case "Boolean":
                        return NUMBER;
                    case "Float":
                        return NUMBER;
                    case "String":
                        return STRING;
                    default:
                        throw new PrestoException(NOT_SUPPORTED, "unsupported SCALAR name: " + ngqlType.name());
                }
            case ENUM:
            case UNION:
            default:
                throw new PrestoException(NOT_SUPPORTED, "unsupported kind: " + ngqlType.name());
        }
    }

    private final NomsType type;
    private final List<NomsType> arguments;
    private final Map<String,NomsType> fields;
    private final Type nativeType;
    private final Class<?> javaType;

    private NomsType(Type nativeType, Class<?> javaType)
    {
        this.type = this;
        this.arguments = Collections.EMPTY_LIST;
        this.fields = Collections.EMPTY_MAP;
        this.nativeType = nativeType;
        this.javaType = javaType;
    }

    private NomsType(NomsType type, List<NomsType> arguments)
    {
        this(type, arguments, Collections.EMPTY_MAP);
    }

    private NomsType(
            NomsType type,
            List<NomsType> arguments,
            Map<String,NomsType> fields
    ) {
        this.type = type;
        this.arguments = arguments;
        this.fields = fields;
        this.nativeType = type.nativeType;
        this.javaType = type.javaType;
    }

    public boolean typeOf(NomsType... types) {
        return Arrays.binarySearch(types, type) > -1;
    }

    public Type getNativeType()
    {
        return nativeType;
    }

    public List<NomsType> getTypeArguments() { return arguments; }

    public Map<String,NomsType> getFields() { return fields; }

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
        else if (nomsType.typeOf(STRING)) {
            return NullableValue.of(nativeType, utf8Slice(row.getString(i)));
        } else if (nomsType.typeOf(BOOLEAN)) {
            return NullableValue.of(nativeType, row.getBool(i));
        } else if (nomsType.typeOf(NUMBER)) {
            return NullableValue.of(nativeType, row.getDouble(i));
        } else if (nomsType.typeOf(BLOB, SET)) {
            checkTypeArguments(nomsType, 1, typeArguments);
            return NullableValue.of(nativeType, utf8Slice(buildSetValue(row, i, typeArguments.get(0))));
        } else if (nomsType.typeOf(LIST)) {
            checkTypeArguments(nomsType, 1, typeArguments);
            return NullableValue.of(nativeType, utf8Slice(buildListValue(row, i, typeArguments.get(0))));
        } else if (nomsType.typeOf(MAP)) {
            checkTypeArguments(nomsType, 2, typeArguments);
            return NullableValue.of(nativeType, utf8Slice(buildMapValue(row, i, typeArguments.get(0), typeArguments.get(1))));
        } else {
            throw new IllegalStateException("Handling of type " + nomsType + " is not implemented");
        }
    }

    private static String buildSetValue(Row row, int i, NomsType elemType)
    {
        return buildArrayValue(row.getSet(i, elemType.javaType), elemType);
    }

    private static String buildListValue(Row row, int i, NomsType elemType)
    {
        return buildArrayValue(row.getList(i, elemType.javaType), elemType);
    }

    private static String buildMapValue(Row row, int i, NomsType keyType, NomsType valueType)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<?, ?> entry : row.getMap(i, keyType.javaType, valueType.javaType).entrySet()) {
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

    private static void checkTypeArguments(NomsType type, int expectedSize,
                                           List<NomsType> typeArguments)
    {
        if (typeArguments == null || typeArguments.size() != expectedSize) {
            throw new IllegalArgumentException("Wrong number of type arguments " + typeArguments
                    + " for " + type);
        }
    }

    private static String objectToString(Object object, NomsType elemType)
    {
        if (elemType.typeOf(STRING)) {
            return CassandraCqlUtils.quoteStringLiteralForJson(object.toString());
        } else if (elemType.typeOf(BLOB)) {
            return CassandraCqlUtils.quoteStringLiteralForJson(Bytes.toHexString((ByteBuffer) object));
        } else if (elemType.typeOf(BOOLEAN, NUMBER)) {
            return object.toString();
        } else {
            throw new IllegalStateException("Handling of type " + elemType + "[" + elemType.type + "] is not implemented");
        }
    }

    public Object getJavaValue(Object nativeValue)
    {
        if (this.typeOf(STRING)) {
            return ((Slice) nativeValue).toStringUtf8();
        } else if (this.typeOf(BOOLEAN, NUMBER, BLOB)) {
            return ((Slice) nativeValue).toStringUtf8();
        } else {
            throw new IllegalStateException("Back conversion not implemented for " + this);
        }
    }

    public static NomsType toNomsType(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return BOOLEAN;
        }
        else if (type.equals(BigintType.BIGINT)) {
            return NUMBER;
        }
        else if (type.equals(IntegerType.INTEGER)) {
            return NUMBER;
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return NUMBER;
        }
        else if (type.equals(RealType.REAL)) {
            return NUMBER;
        }
        else if (isVarcharType(type)) {
            return STRING;
        }
        else if (type.equals(DateType.DATE)) {
            return STRING;
        }
        else if (type.equals(VarbinaryType.VARBINARY)) {
            return BLOB;
        }
        else if (type.equals(TimestampType.TIMESTAMP)) {
            return STRING;
        }
        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    public int compareTo(NomsType other) {
        return System.identityHashCode(type) - System.identityHashCode(other.type);
    }
}
