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

import com.facebook.presto.noms.util.CassandraCqlUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects.ToStringHelper;

import java.util.List;
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

    @JsonCreator
    public NomsColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("name") String name,
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("nomsType") NomsType nomsType)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.name = requireNonNull(name, "name is null");
        checkArgument(ordinalPosition >= 0, "ordinalPosition is negative");
        this.ordinalPosition = ordinalPosition;
        this.nomsType = requireNonNull(nomsType, "nomsType is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public NomsType getNomsType()
    {
        return nomsType;
    }

    @JsonProperty
    public List<NomsType> getTypeArguments()
    {
        return nomsType.getTypeArguments();
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(CassandraCqlUtils.cqlNameToSqlName(name), nomsType.getNativeType(), null, false);
    }

    public Type getType()
    {
        return nomsType.getNativeType();
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

        if (!nomsType.getTypeArguments().isEmpty()) {
            helper.add("typeArguments", nomsType.getTypeArguments());
        }

        return helper.toString();
    }
}
