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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class NomsColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final String name;
    private final int ordinalPosition;
    private final NomsType nomsType;
    private final boolean primaryKey;

    @JsonCreator
    public NomsColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("name") String name,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("nomsType") NomsType nomsType,
            @JsonProperty("primaryKey") boolean primaryKey)
    {
        this.connectorId = requireNonNull(connectorId);
        this.name = requireNonNull(name);
        checkArgument(ordinalPosition >= 0, "ordinalPosition is negative");
        this.ordinalPosition = ordinalPosition;
        this.nomsType = requireNonNull(nomsType);
        this.primaryKey = primaryKey;
    }

    @JsonProperty
    public String connectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String name()
    {
        return name;
    }

    @JsonProperty
    public int ordinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public boolean isPrimaryKey()
    {
        return primaryKey;
    }

    @JsonProperty
    public NomsType nomsType()
    {
        return nomsType;
    }

    public ColumnMetadata columnMetadata()
    {
        return new ColumnMetadata(name, nomsType.nativeType(), null, false);
    }

    public Type type()
    {
        return nomsType.nativeType();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                connectorId,
                name,
                ordinalPosition,
                nomsType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NomsColumnHandle other = (NomsColumnHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equals(this.nomsType, other.nomsType);
    }

    @Override
    public String toString()
    {
        ToStringHelper helper = toStringHelper(this)
                .add("connectorId", connectorId)
                .add("name", name)
                .add("ordinalPosition", ordinalPosition)
                .add("nomsType", nomsType);

        if (!nomsType.arguments().isEmpty()) {
            helper.add("typeArguments", nomsType.arguments());
        }

        return helper.toString();
    }
}
