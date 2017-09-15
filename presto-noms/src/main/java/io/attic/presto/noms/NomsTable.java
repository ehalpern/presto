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

import com.google.common.collect.ImmutableList;
import io.attic.presto.noms.ngql.NomsSchema;

import java.net.URI;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class NomsTable
{
    private final NomsTableHandle tableHandle;
    private final NomsSchema schema;

    private final List<NomsColumnHandle> columns;
    private final URI source;

    public NomsTable(NomsTableHandle tableHandle, NomsSchema schema, List<NomsColumnHandle> columns, URI source)
    {
        this.tableHandle = tableHandle;
        this.schema = schema;
        this.columns = ImmutableList.copyOf(columns);
        this.source = source;
    }

    public List<NomsColumnHandle> columns()
    {
        return columns;
    }

    public NomsTableHandle tableHandle()
    {
        return tableHandle;
    }

    public NomsSchema schema()
    {
        return schema;
    }

    public URI source()
    {
        return source;
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
