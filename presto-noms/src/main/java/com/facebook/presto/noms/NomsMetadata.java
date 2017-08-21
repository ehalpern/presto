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
    private final NomsSession nomsSession;

    @Inject
    public NomsMetadata(
            NomsConnectorId connectorId,
            NomsSession nomsSession)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nomsSession = requireNonNull(nomsSession, "nomsSession is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return nomsSession.getSchemaNames().stream()
                .map(name -> name.toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }

    @Override
    public NomsTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        try {
            return nomsSession.getTable(tableName).getTableHandle();
        }
        catch (TableNotFoundException | SchemaNotFoundException e) {
            // table was not found
            return null;
        }
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        return ((NomsTableHandle) tableHandle).getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        return getTableMetadata(getTableName(tableHandle));
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        NomsTable table = nomsSession.getTable(tableName);
        List<ColumnMetadata> columns = table.getColumns().stream()
                .map(NomsColumnHandle::getColumnMetadata)
                .collect(toList());
        return new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            try {
                for (String tableName : nomsSession.getTableNames(schemaName)) {
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
        requireNonNull(session, "session is null");
        requireNonNull(tableHandle, "tableHandle is null");
        NomsTable table = nomsSession.getTable(getTableName(tableHandle));
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (NomsColumnHandle columnHandle : table.getColumns()) {
            columnHandles.put(CassandraCqlUtils.cqlNameToSqlName(columnHandle.getName()).toLowerCase(ENGLISH), columnHandle);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (NotFoundException e) {
                // table disappeared during listing operation
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
        return ((NomsColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        NomsTableHandle tableHandle = (NomsTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new NomsTableLayoutHandle(tableHandle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
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

    /*
   @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(NOT_SUPPORTED, "CREATE TABLE not yet supported for Cassandra");
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkArgument(tableHandle instanceof NomsTableHandle, "tableHandle is not an instance of NomsTableHandle");

        if (!allowDropTable) {
            throw new PrestoException(PERMISSION_DENIED, "DROP TABLE is disabled in this Cassandra catalog");
        }

        NomsTableHandle nomsTableHandle = (NomsTableHandle) tableHandle;
        String schemaName = nomsTableHandle.getSchemaName();
        String tableName = nomsTableHandle.getTableName();

        nomsSession.execute(String.format("DROP TABLE \"%s\".\"%s\"", schemaName, tableName));
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        throw new PrestoException(NOT_SUPPORTED, "Renaming tables not yet supported for Cassandra");
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }

        // get the root directory for the database
        SchemaTableName table = tableMetadata.getTable();
        String schemaName = nomsSession.getCaseSensitiveSchemaName(table.getSchemaName());
        String tableName = table.getTableName();
        List<String> columns = columnNames.build();
        List<Type> types = columnTypes.build();
        StringBuilder queryBuilder = new StringBuilder(String.format("CREATE TABLE \"%s\".\"%s\"(id uuid primary key", schemaName, tableName));
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i);
            Type type = types.get(i);
            queryBuilder.append(", ")
                    .append(name)
                    .append(" ")
                    .append(toCassandraType(type).name().toLowerCase(ENGLISH));
        }
        queryBuilder.append(") ");

        // We need to create the Cassandra table before commit because the record needs to be written to the table.
        nomsSession.execute(queryBuilder.toString());
        return new NomsOutputTableHandle(
                connectorId,
                schemaName,
                tableName,
                columnNames.build(),
                columnTypes.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        NomsTableHandle table = (NomsTableHandle) tableHandle;
        SchemaTableName schemaTableName = new SchemaTableName(table.getSchemaName(), table.getTableName());
        List<NomsColumnHandle> columns = nomsSession.getTable(schemaTableName).getColumns();
        List<String> columnNames = columns.stream().map(NomsColumnHandle::getName).map(CassandraCqlUtils::validColumnName).collect(Collectors.toList());
        List<Type> columnTypes = columns.stream().map(NomsColumnHandle::getType).collect(Collectors.toList());

        return new NomsInsertTableHandle(
                connectorId,
                validSchemaName(table.getSchemaName()),
                validTableName(table.getTableName()),
                columnNames,
                columnTypes);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        return Optional.empty();
    }
    */
}
