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
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.noms.util.CassandraCqlUtils.toCQLCompatibleString;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class NomsPartitionManager
{
    private static final Logger log = Logger.get(NomsPartitionManager.class);

    private final NomsSession nomsSession;

    @Inject
    public NomsPartitionManager(NomsSession nomsSession)
    {
        this.nomsSession = requireNonNull(nomsSession, "nomsSession is null");
    }

    public NomsPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain)
    {
        NomsTableHandle nomsTableHandle = (NomsTableHandle) tableHandle;

        NomsTable table = nomsSession.getTable(nomsTableHandle.getSchemaTableName());
        List<NomsColumnHandle> partitionKeys = table.getPartitionKeyColumns();

        // fetch the partitions
        List<NomsPartition> allPartitions = getCassandraPartitions(table, tupleDomain);
        log.debug("%s.%s #partitions: %d", nomsTableHandle.getSchemaName(), nomsTableHandle.getTableName(), allPartitions.size());

        // do a final pass to filter based on fields that could not be used to build the prefix
        List<NomsPartition> partitions = allPartitions.stream()
                .filter(partition -> tupleDomain.overlaps(partition.getTupleDomain()))
                .collect(toList());

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain<ColumnHandle> remainingTupleDomain = TupleDomain.none();
        if (!tupleDomain.isNone()) {
            if (partitions.size() == 1 && partitions.get(0).isUnpartitioned()) {
                remainingTupleDomain = tupleDomain;
            }
            else {
                @SuppressWarnings({"rawtypes", "unchecked"})
                List<ColumnHandle> partitionColumns = (List) partitionKeys;
                remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains().get(), not(in(partitionColumns))));
            }
        }

        // push down indexed column fixed value predicates only for unpartitioned partition which uses token range query
        if ((partitions.size() == 1) && partitions.get(0).isUnpartitioned()) {
            Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().get();
            List<ColumnHandle> indexedColumns = new ArrayList<>();
            // compose partitionId by using indexed column
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                NomsColumnHandle column = (NomsColumnHandle) entry.getKey();
                Domain domain = entry.getValue();
                if (column.isIndexed() && domain.isSingleValue()) {
                    sb.append(CassandraCqlUtils.validColumnName(column.getName()))
                            .append(" = ")
                            .append(CassandraCqlUtils.cqlValue(toCQLCompatibleString(entry.getValue().getSingleValue()), column.getNomsType()));
                    indexedColumns.add(column);
                    // Only one indexed column predicate can be pushed down.
                    break;
                }
            }
            if (sb.length() > 0) {
                NomsPartition partition = partitions.get(0);
                TupleDomain<ColumnHandle> filterIndexedColumn = TupleDomain.withColumnDomains(Maps.filterKeys(remainingTupleDomain.getDomains().get(), not(in(indexedColumns))));
                partitions = new ArrayList<>();
                partitions.add(new NomsPartition(partition.getKey(), sb.toString(), filterIndexedColumn, true));
                return new NomsPartitionResult(partitions, filterIndexedColumn);
            }
        }
        return new NomsPartitionResult(partitions, remainingTupleDomain);
    }

    private List<NomsPartition> getCassandraPartitions(NomsTable table, TupleDomain<ColumnHandle> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return ImmutableList.of();
        }

        Set<List<Object>> partitionKeysSet = getPartitionKeysSet(table, tupleDomain);

        // empty filter means, all partitions
        if (partitionKeysSet.isEmpty()) {
            return nomsSession.getPartitions(table, ImmutableList.of());
        }

        ImmutableList.Builder<NomsPartition> partitions = ImmutableList.builder();
        for (List<Object> partitionKeys : partitionKeysSet) {
            partitions.addAll(nomsSession.getPartitions(table, partitionKeys));
        }

        return partitions.build();
    }

    private static Set<List<Object>> getPartitionKeysSet(NomsTable table, TupleDomain<ColumnHandle> tupleDomain)
    {
        ImmutableList.Builder<Set<Object>> partitionColumnValues = ImmutableList.builder();
        for (NomsColumnHandle columnHandle : table.getPartitionKeyColumns()) {
            Domain domain = tupleDomain.getDomains().get().get(columnHandle);

            // if there is no constraint on a partition key, return an empty set
            if (domain == null) {
                return ImmutableSet.of();
            }

            // todo does noms allow null partition keys?
            if (domain.isNullAllowed()) {
                return ImmutableSet.of();
            }

            Set<Object> values = domain.getValues().getValuesProcessor().transform(
                    ranges -> {
                        ImmutableSet.Builder<Object> columnValues = ImmutableSet.builder();
                        for (Range range : ranges.getOrderedRanges()) {
                            // if the range is not a single value, we can not perform partition pruning
                            if (!range.isSingleValue()) {
                                return ImmutableSet.of();
                            }
                            Object value = range.getSingleValue();

                            NomsType valueType = columnHandle.getNomsType();
                            columnValues.add(valueType.validatePartitionKey(value));
                        }
                        return columnValues.build();
                    },
                    discreteValues -> {
                        if (discreteValues.isWhiteList()) {
                            return ImmutableSet.copyOf(discreteValues.getValues());
                        }
                        return ImmutableSet.of();
                    },
                    allOrNone -> ImmutableSet.of());
            partitionColumnValues.add(values);
        }
        return Sets.cartesianProduct(partitionColumnValues.build());
    }
}
