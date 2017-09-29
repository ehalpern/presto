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
package io.attic.presto.noms;

import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.google.common.annotations.VisibleForTesting;

import javax.xml.bind.DatatypeConverter;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NomsType
{
    public enum Kind
    {
        Blob(VarbinaryType.VARBINARY),
        Cycle(VarbinaryType.VARBINARY),
        Boolean(BooleanType.BOOLEAN),
        Number(DoubleType.DOUBLE),
        String(VarcharType.VARCHAR),
        List(VarcharType.VARCHAR),
        Map(VarcharType.VARCHAR),
        Ref(VarcharType.VARCHAR),
        Set(VarcharType.VARCHAR),
        Struct(VarcharType.VARCHAR),
        Type(VarcharType.VARCHAR),
        Union(VarcharType.VARCHAR);

        private final Type nativeType;

        private Kind(Type nativeType)
        {
            this.nativeType = nativeType;
        }

        public Type nativeType()
        {
            return nativeType;
        }
    }

    public static final NomsType BLOB = new NomsType(Kind.Blob);
    public static final NomsType BOOLEAN = new NomsType(Kind.Boolean);
    public static final NomsType CYCLE = new NomsType(Kind.Cycle);
    public static final NomsType NUMBER = new NomsType(Kind.Number);
    public static final NomsType STRING = new NomsType(Kind.String);
    public static final NomsType LIST = new NomsType(Kind.List);
    public static final NomsType MAP = new NomsType(Kind.Map);
    public static final NomsType REF = new NomsType(Kind.Ref);
    public static final NomsType SET = new NomsType(Kind.Set);
    public static final NomsType STRUCT = new NomsType(Kind.Struct);
    public static final NomsType EMPTY_LIST = new NomsType("EmptyList", Kind.List);
    public static final NomsType EMPTY_MAP = new NomsType("EmptyList", Kind.Map);
    public static final NomsType EMPTY_SET = new NomsType("EmptySet", Kind.Set);

    private final String name;
    private final Kind kind;
    private final List<NomsType> arguments;
    private final Map<String, NomsType> fields;

    @JsonCreator
    public NomsType(
            @JsonProperty("name") String name,
            @JsonProperty("kind") Kind kind,
            @JsonProperty("arguments") List<NomsType> arguments,
            @JsonProperty("fields") Map<String, NomsType> fields)
    {
        this.name = name;
        this.kind = kind;
        this.arguments = (arguments == null) ? Collections.emptyList() : arguments;
        this.fields = (fields == null) ? Collections.emptyMap() : fields;
    }

    public NomsType(String name, Kind kind, List<NomsType> arguments)
    {
        this(name, kind, arguments, Collections.emptyMap());
    }

    private NomsType(Kind kind)
    {
        this(kind.name(), kind);
    }

    private NomsType(String name, Kind kind)
    {
        this(name, kind, Collections.emptyList(), Collections.emptyMap());
    }

    @JsonProperty
    public String name()
    {
        return name;
    }

    @JsonProperty
    public Kind kind()
    {
        return kind;
    }

    @JsonProperty
    public List<NomsType> arguments()
    {
        return arguments;
    }

    public NomsType argument(int position)
    {
        return arguments.get(position);
    }

    @JsonProperty
    public Map<String, NomsType> fields()
    {
        return fields;
    }

    public Type nativeType()
    {
        return kind.nativeType();
    }

    public int hashCode()
    {
        return name.hashCode() ^ kind.hashCode();
    }

    public boolean equals(Object o)
    {
        NomsType other = (NomsType) o;
        return kind == other.kind && name.equals(other.name);
    }

    @VisibleForTesting
    public static String buildArrayValue(Collection<?> collection, NomsType elemType)
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

    public static String objectToString(Object object, NomsType elemType)
    {
        switch (elemType.kind()) {
            case String:
                return quoteStringLiteralForJson(object.toString());
            case Blob:
                return quoteStringLiteralForJson(DatatypeConverter.printHexBinary(((ByteBuffer) object).array()));
            case Boolean:
            case Number:
                return object.toString();
            default:
                throw new IllegalStateException("Handling of type " + elemType + " is not implemented");
        }
    }

    private static String quoteStringLiteralForJson(String string)
    {
        return '"' + new String(JsonStringEncoder.getInstance().quoteAsUTF8(string)) + '"';
    }
}
