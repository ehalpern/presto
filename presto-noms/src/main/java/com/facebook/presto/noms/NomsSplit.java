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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class NomsSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final HostAddress address;
    private final String schema;
    private final String table;

    @JsonCreator
    public NomsSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("address") HostAddress address)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
        requireNonNull(address, "address is null");

        this.connectorId = connectorId;
        this.schema = schema;
        this.table = table;
        this.address = address;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(address);
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("host", address)
                .put("schema", schema)
                .put("table", table)
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(table)
                .toString();
    }
    public NomsTableHandle getCassandraTableHandle()
    {
        return new NomsTableHandle(connectorId, schema, table);
    }
}
