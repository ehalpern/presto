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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class NomsRecordSet
        implements RecordSet
{
    private final NomsSession nomsSession;
    private final List<NomsType> nomsTypes;
    private final List<Type> columnTypes;

    public NomsRecordSet(NomsSession nomsSession, List<NomsColumnHandle> nomsColumns)
    {
        this.nomsSession = requireNonNull(nomsSession, "nomsSession is null");
        requireNonNull(nomsColumns, "nomsColumns is null");
        this.nomsTypes = transformList(nomsColumns, NomsColumnHandle::getNomsType);
        this.columnTypes = transformList(nomsColumns, NomsColumnHandle::getType);
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new NomsRecordCursor(nomsSession, nomsTypes);
    }

    private static <T, R> List<R> transformList(List<T> list, Function<T, R> function)
    {
        return ImmutableList.copyOf(list.stream().map(function).collect(toList()));
    }
}
