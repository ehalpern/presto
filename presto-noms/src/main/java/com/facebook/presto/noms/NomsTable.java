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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.stream.Collectors.toList;

public class NomsTable
{
    private final NomsTableHandle tableHandle;
    private final List<NomsColumnHandle> columns;

    public NomsTable(NomsTableHandle tableHandle, List<NomsColumnHandle> columns)
    {
        this.tableHandle = tableHandle;
        this.columns = ImmutableList.copyOf(columns);
    }

    public List<NomsColumnHandle> getColumns()
    {
        return columns;
    }

    public NomsTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public List<NomsColumnHandle> getPartitionKeyColumns()
    {
        return columns.stream()
                .filter(NomsColumnHandle::isPartitionKey)
                .collect(toList());
    }

    public List<NomsColumnHandle> getClusteringKeyColumns()
    {
        return columns.stream()
                .filter(NomsColumnHandle::isClusteringKey)
                .collect(toList());
    }

    public String getTokenExpression()
    {
        StringBuilder sb = new StringBuilder();
        for (NomsColumnHandle column : getPartitionKeyColumns()) {
            if (sb.length() == 0) {
                sb.append("token(");
            }
            else {
                sb.append(",");
            }
            sb.append(CassandraCqlUtils.validColumnName(column.getName()));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return tableHandle.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof NomsTable)) {
            return false;
        }
        NomsTable that = (NomsTable) obj;
        return this.tableHandle.equals(that.tableHandle);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableHandle", tableHandle)
                .toString();
    }
}
