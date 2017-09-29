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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class NomsMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final NomsSession session;

    @Inject
    public NomsMetadata(
            NomsConnectorId connectorId,
            NomsSession session)
    {
        this.connectorId = requireNonNull(connectorId).toString();
        this.session = requireNonNull(session);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return this.session.getSchemaNames().stream()
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }

    @Override
    public NomsTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName);
        try {
            return this.session.getTableHandle(tableName);
        }
        catch (TableNotFoundException | SchemaNotFoundException e) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName name = ((NomsTableHandle) requireNonNull(tableHandle)).getSchemaTableName();
        return getTableMetadata(name);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        NomsTable table = session.getTable(session.getTableHandle(tableName));
        List<ColumnMetadata> columns = table.columns().stream()
                .map(NomsColumnHandle::columnMetadata)
                .collect(toList());
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            try {
                for (String tableName : this.session.getTableNames(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName.toLowerCase(ENGLISH)));
                }
            }
            catch (SchemaNotFoundException e) {
                // schema disappeared during listing operation
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(session);
        requireNonNull(tableHandle);
        NomsTable table = this.session.getTable((NomsTableHandle) tableHandle);
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (NomsColumnHandle columnHandle : table.columns()) {
            columnHandles.put(columnHandle.name().toLowerCase(ENGLISH), columnHandle);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix);
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (NotFoundException e) {
                // rows disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((NomsColumnHandle) columnHandle).columnMetadata();
    }

    /**
     * Given the table, columns and constraints of a query, returns layout
     * information to be used by presto to execute and (potentially) distribute
     * the query.
     * <p>
     * Consider the example of HiveMetadata.getTableLayouts:
     * - If the query constrains the partition key, issue a query to
     * determine the distinct partition keys matching the predicate.
     * - If the query constrains the cluster key (by specifying exact
     * values), determine the buckets for these values
     * - Return a layout that specifies the new query (minus the partition
     * key predicates), the list of partitions and the list of buckets
     * <p>
     * TBD: For the first cut of noms, we'll forgo the partition and use the
     * primary key as the cluster key. This means there will one partition but
     * multiple buckets to split on.
     */
    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        // NOTE: On passing effectivePredicate:
        // Hive does the following:
        // - HivePartition (created by partitionManager.getPartitions(..., constraint) contains
        //   - effectivePredicate (= constraint)
        //   - partitionId
        //   - buckets
        // - HiveSplitManager.getSplits
        //   - Retreives partitions from layout
        //   - Stores effective predicate in split?
        return ImmutableList.of(new ConnectorTableLayoutResult(
                new ConnectorTableLayout(new NomsTableLayoutHandle((NomsTableHandle) table, constraint.getSummary())),
                constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .toString();
    }

    // exposed for testing
    /*package*/ NomsSession session()
    {
        return session;
    }

    // See ConnectorMetadata super class for available row management and insert hooks
}
