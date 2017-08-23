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

import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public enum RootNomsType
        implements NomsType
{
    Blob(VarbinaryType.VARBINARY, ByteBuffer.class),
    Cycle(VarbinaryType.VARBINARY, null), // TODO: verify native type
    Boolean(BooleanType.BOOLEAN, Boolean.class),
    Number(DoubleType.DOUBLE, Double.class),
    String(VarcharType.VARCHAR, String.class),
    List(VarcharType.VARCHAR, null),    // TODO: Why not List?
    Map(VarcharType.VARCHAR, null),    // TODO: Why not Map?
    Ref(VarcharType.VARCHAR, null),     // TODO: Verify native type
    Set(VarcharType.VARCHAR, null),     // TODO: Why not Set?
    Struct(VarcharType.VARCHAR, null),  // TODO: Why not Row?
    Type(VarcharType.VARCHAR, null),    // TODO: Verify native type
    Union(VarcharType.VARCHAR, null);   // TODO: determine native type

    private final Type nativeType;
    private final Class<?> javaType;

    private RootNomsType(Type nativeType, Class<?> javaType)
    {
        this.nativeType = nativeType;
        this.javaType = javaType;
    }

    public boolean typeOf(RootNomsType... rootTypes)
    {
        return Arrays.binarySearch(rootTypes, this) > -1;
    }

    public String getName()
    {
        return this.name();
    }

    public RootNomsType getRootType()
    {
        return this;
    }

    public Type getNativeType()
    {
        return nativeType;
    }

    public Class<?> getJavaType()
    {
        return javaType;
    }

    public List<NomsType> getArguments()
    {
        return Collections.EMPTY_LIST;
    }

    public Map<String, NomsType> getFields()
    {
        return Collections.EMPTY_MAP;
    }
}
