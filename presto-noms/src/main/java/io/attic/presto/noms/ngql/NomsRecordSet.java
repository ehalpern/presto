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
package io.attic.presto.noms.ngql;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.attic.presto.noms.NomsColumnHandle;
import io.attic.presto.noms.NomsSession;
import io.attic.presto.noms.NomsSplit;
import io.attic.presto.noms.NomsTable;

import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class NomsRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(NomsRecordSet.class);

    private final NomsSession session;
    private final NomsTable table;
    private final List<Type> columnTypes;
    private final List<NomsColumnHandle> columns;
    private final NomsSplit split;

    /*package*/ NomsRecordSet(NomsSession session, NomsSplit split, NomsTable table, List<NomsColumnHandle> columns)
    {
        this.session = requireNonNull(session, "session is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.columnTypes = transformList(columns, NomsColumnHandle::getType);
        this.table = table;
        this.split = split;
        log.debug("Creating RecordSet for: %s", split);
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new NomsRecordCursor(session, split, table, columns);
    }

    private static <T, R> List<R> transformList(List<T> list, Function<T, R> function)
    {
        return ImmutableList.copyOf(list.stream().map(function).collect(toList()));
    }
}
