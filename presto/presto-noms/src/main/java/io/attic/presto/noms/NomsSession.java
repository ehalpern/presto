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

import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.attic.presto.noms.ngql.SchemaQuery;
import io.attic.presto.noms.util.NomsRunner;
import io.attic.presto.noms.util.NomsServer;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class NomsSession
{
    private static final Logger log = Logger.get(NomsSession.class);

    private final String connectorId;
    private final URI nomsURI;
    private final NomsClientConfig config;
    private final NomsServer server;

    public NomsSession(String connectorId, NomsClientConfig config)
    {
        this.connectorId = requireNonNull(connectorId);
        this.config = requireNonNull(config);
        if (config.isAutostart()) {
            String dbSpec = config.getDatabasePrefix() + "/" + config.getDatabase();
            this.server = NomsServer.start(dbSpec);
            this.nomsURI = this.server.uri();
        }
        else {
            server = null;
            this.nomsURI = config.getURI();
        }
    }

    public NomsClientConfig config()
    {
        return config;
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
        // Use `noms ds` for now.
        return NomsRunner.ds(nomsURI.toString());
    }

    public NomsTableHandle getTableHandle(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        if (!getTableNames(schemaTableName.getSchemaName()).contains(schemaTableName.getTableName())) {
            throw new TableNotFoundException(schemaTableName);
        }
        return new NomsTableHandle(connectorId, config.getDatabase(), schemaTableName.getTableName());
    }

    public NomsTable getTable(NomsTableHandle tableHandle)
    {
        ImmutableList.Builder<NomsColumnHandle> columnHandles = ImmutableList.builder();

        NomsSchema schema = querySchema(tableHandle.getTableName());
        int i = 0;
        for (Pair<String, NomsType> p : schema.columns()) {
            columnHandles.add(new NomsColumnHandle(connectorId, p.getKey(), i++, p.getValue(), false));
        }
        return new NomsTable(
                new NomsTableHandle(connectorId, config.getDatabase(), tableHandle.getTableName()),
                schema,
                columnHandles.build());
    }

    public NomsSchema querySchema(String table)
    {
        return execute(table, SchemaQuery.create());
    }

    public <Q extends NomsQuery<R>, R extends NomsQuery.Result> R execute(String table, Q query)
    {
        try {
            // TODO: retry
            return query.execute(nomsURI, table);
        }
        catch (IOException e) {
            // TODO: better error handling
            throw new RuntimeException(e);
        }
    }

    public void close()
    {
        if (server != null) {
            server.stop();
        }
    }
}
