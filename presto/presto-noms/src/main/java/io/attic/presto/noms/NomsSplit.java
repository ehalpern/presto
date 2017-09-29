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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class NomsSplit
        implements ConnectorSplit
{
    private final List<HostAddress> addresses;
    private final SchemaTableName tableName;
    private final TupleDomain<NomsColumnHandle> effectivePredicate;
    private final long offset;
    private final long limit;

    @JsonCreator
    public NomsSplit(
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("tableName") SchemaTableName tableName,
            @JsonProperty("effectivePredicate") TupleDomain<NomsColumnHandle> effectivePredicate,
            @JsonProperty("offset") long offset,
            @JsonProperty("limit") long limit)
    {
        this.addresses = requireNonNull(addresses);
        this.tableName = requireNonNull(tableName);
        this.effectivePredicate = requireNonNull(effectivePredicate);
        checkArgument(offset >= 0, "offset:%s >= 0");
        this.offset = offset;
        this.limit = limit;
    }

    @JsonProperty
    public SchemaTableName getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<NomsColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    /**
     * Returns false to indicate the split is pinned to a worker (running at
     * one of |addresses|).
     */
    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    /**
     * The node addresses where the split may run
     */
    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @JsonProperty
    public long getOffset()
    {
        return offset;
    }

    @JsonProperty
    public long getLimit()
    {
        return limit;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("addresses", addresses)
                .add("tableName", tableName)
                .add("offset", offset)
                .add("limit", limit)
                .toString();
    }
}
