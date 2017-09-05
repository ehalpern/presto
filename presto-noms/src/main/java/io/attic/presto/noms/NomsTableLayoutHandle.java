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
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class NomsTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final NomsTableHandle table;
    private final TupleDomain<ColumnHandle> effectivePredicate;

    @JsonCreator
    public NomsTableLayoutHandle(
            @JsonProperty("table") NomsTableHandle table,
            @JsonProperty("effectivePredicate") TupleDomain<ColumnHandle> effectivePredicate)
    {
        this.table = requireNonNull(table, "table is null");
        this.effectivePredicate = requireNonNull(effectivePredicate, "effectivePredicate is null");
    }

    @JsonProperty
    public NomsTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getEffectivePredicate()
    {
        return effectivePredicate;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NomsTableLayoutHandle that = (NomsTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(effectivePredicate, that.effectivePredicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, effectivePredicate);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
