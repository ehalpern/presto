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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class NomsTypeWithTypeArguments
        implements FullNomsType
{
    private final NomsType nomsType;
    private final List<NomsType> typeArguments;

    public NomsTypeWithTypeArguments(NomsType nomsType, List<NomsType> typeArguments)
    {
        this.nomsType = requireNonNull(nomsType, "nomsType is null");
        this.typeArguments = requireNonNull(typeArguments, "typeArguments is null");
    }

    @Override
    public NomsType getNomsType()
    {
        return nomsType;
    }

    @Override
    public List<NomsType> getTypeArguments()
    {
        return typeArguments;
    }

    @Override
    public String toString()
    {
        if (typeArguments != null) {
            return nomsType.toString() + typeArguments.toString();
        }
        else {
            return nomsType.toString();
        }
    }
}
