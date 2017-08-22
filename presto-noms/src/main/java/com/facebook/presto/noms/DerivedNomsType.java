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

import com.facebook.presto.spi.type.Type;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DerivedNomsType
        implements NomsType
{
    public static final NomsType EMPTY_LIST = new DerivedNomsType("EmptyList", RootNomsType.List);
    public static final NomsType EMPTY_MAP = new DerivedNomsType("EmptyList", RootNomsType.Map);
    public static final NomsType EMPTY_SET = new DerivedNomsType("EmptySet", RootNomsType.Set);

    private final String name;
    private final RootNomsType rootType;
    private final List<NomsType> arguments;
    private final Map<String, NomsType> fields;

    private DerivedNomsType(String name, RootNomsType type)
    {
        this(name, type, Collections.EMPTY_LIST, Collections.EMPTY_MAP);
    }

    /*package*/ DerivedNomsType(String name, RootNomsType type, List<NomsType> arguments)
    {
        this(name, type, arguments, Collections.EMPTY_MAP);
    }

    /*package*/ DerivedNomsType(String name, RootNomsType rootType, List<NomsType> arguments, Map<String, NomsType> fields)
    {
        this.name = name;
        this.rootType = rootType;
        this.arguments = arguments;
        this.fields = fields;
    }

    public boolean typeOf(RootNomsType... rootTypes)
    {
        return Arrays.binarySearch(rootTypes, rootType) > -1;
    }

    public String getName()
    {
        return name;
    }

    public RootNomsType getRootNomsType()
    {
        return rootType;
    }

    public Type getNativeType()
    {
        return rootType.getNativeType();
    }

    public Class<?> getJavaType()
    {
        return rootType.getJavaType();
    }

    public List<NomsType> getTypeArguments()
    {
        return arguments;
    }

    public Map<String, NomsType> getFields()
    {
        return fields;
    }
}
