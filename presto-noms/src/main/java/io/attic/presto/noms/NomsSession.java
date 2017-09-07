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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.attic.presto.noms.ngql.Ngql;
import io.attic.presto.noms.ngql.NomsSchema;
import io.attic.presto.noms.ngql.SchemaQuery;
import io.attic.presto.noms.util.NomsRunner;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class NomsSession
{
    private static final Logger log = Logger.get(NomsSession.class);

    private final String connectorId;
    private final URI nomsURI;
    private final NomsClientConfig config;

    public NomsSession(String connectorId, NomsClientConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.nomsURI = config.getURI();
        this.config = requireNonNull(config, "config is null");
    }

    public List<String> getSchemaNames()
    {
        return ImmutableList.of(config.getDatabase());
    }

    public List<String> getTableNames(String schemaName)
            throws SchemaNotFoundException
    {
        if (!config.getDatabase().equals(schemaName)) {
            throw new SchemaNotFoundException("Schema '" + schemaName + "' is not defined in configuration");
        }
        // Hack by using noms CLI for now. Would be nice for ngql to provide a dataset query.
        return NomsRunner.ds(nomsURI.toString());
    }

    public NomsTable getTable(SchemaTableName schemaTableName)
            throws SchemaNotFoundException, TableNotFoundException
    {
        if (!getTableNames(schemaTableName.getSchemaName()).contains(schemaTableName.getTableName())) {
            throw new TableNotFoundException(schemaTableName);
        }
        ImmutableList.Builder<NomsColumnHandle> columnHandles = ImmutableList.builder();

        NomsSchema schema = getSchema(schemaTableName.getTableName());
        NomsType tableType = schema.tableType();
        NomsType rowType;
        switch (tableType.kind()) {
            case List: case Set:
                rowType = tableType.arguments().get(0);
                break;
            case Map:
                rowType = tableType.arguments().get(1);
                break;
            default:
                rowType = tableType;
                break;
        }
        switch (rowType.kind()) {
            case Blob: case Boolean: case Number: case String:
                columnHandles.add(new NomsColumnHandle(connectorId, "value", 0, tableType, false));
                break;
            case Struct:
                int pos = 0;
                for (Map.Entry<String, NomsType> e : rowType.fields().entrySet()) {
                    columnHandles.add(new NomsColumnHandle(connectorId, e.getKey(), pos++, e.getValue(), false));
                }
                break;
            default:
                throw new PrestoException(NOT_SUPPORTED, "row type " + rowType + " non supported");
        }
        return new NomsTable(
                new NomsTableHandle(connectorId, config.getDatabase(), schemaTableName.getTableName()),
                schema,
                tableType,
                columnHandles.build(),
                nomsURI);
    }

    private NomsSchema getSchema(String table)
    {
        SchemaQuery query = Ngql.schemaQuery();
        return execute(table, query);
    }

    public <Q extends Ngql.Query<R>, R extends Ngql.Result> R execute(String table, Q query)
    {
        try {
            // TODO: retry
            return Ngql.execute(nomsURI, table, query);
        }
        catch (IOException e) {
            // TODO: better error handling
            throw new RuntimeException(e);
        }
    }

    public PreparedStatement prepare(RegularStatement statement)
    {
        return executeWithSession(session -> session.prepare(statement));
    }

    public ResultSet execute(Statement statement)
    {
        return executeWithSession(session -> session.execute(statement));
    }

    private <T> T executeWithSession(SessionCallable<T> sessionCallable)
    {
        throw new AssertionError("out of order");
    }

    private interface SessionCallable<T>
    {
        T executeWithSession(Session session);
    }
}
