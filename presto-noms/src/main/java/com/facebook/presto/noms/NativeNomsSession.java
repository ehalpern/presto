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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.facebook.presto.noms.util.*;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.log.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.Select.Where;
import static com.facebook.presto.noms.util.CassandraCqlUtils.validSchemaName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class NativeNomsSession
        implements NomsSession
{
    private static final Logger log = Logger.get(NativeNomsSession.class);

    private final String connectorId;
    private final NomsClientConfig config;

    public NativeNomsSession(String connectorId, NomsClientConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public String getCaseSensitiveSchemaName(String caseInsensitiveSchemaName)
    {
        return getKeyspaceByCaseInsensitiveName(caseInsensitiveSchemaName).getName();
    }

    @Override
    public List<String> getCaseSensitiveSchemaNames()
    {
        return ImmutableList.of(config.getDatabase());
    }

    @Override
    public List<String> getCaseSensitiveTableNames(String caseInsensitiveSchemaName)
            throws SchemaNotFoundException
    {
        KeyspaceMetadata keyspace = getKeyspaceByCaseInsensitiveName(caseInsensitiveSchemaName);
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (TableMetadata table : keyspace.getTables()) {
            builder.add(table.getName());
        }
        return builder.build();
    }

    @Override
    public NomsTable getTable(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        ImmutableList.Builder<NomsColumnHandle> columnHandles = ImmutableList.builder();
        try {
            NgqlSchema schema = NgqlUtils.introspectQuery(config.getNgqlURI(), schemaTableName.getTableName());
            NomsType rootType = NomsType.from(schema.rootValueType());
            NomsType rowType = rootType;
            if (rootType.typeOf(NomsType.LIST, NomsType.SET)) {
                rowType = rootType.getTypeArguments().get(0);
            } else if (rootType.typeOf(NomsType.MAP)) {
                rowType = rootType.getTypeArguments().get(1);
            }
            if (rowType.typeOf(NomsType.BLOB, NomsType.BOOLEAN, NomsType.NUMBER, NomsType.STRING)) {
                columnHandles.add(new NomsColumnHandle(connectorId, "value", 0, rootType));
            } else if (rowType.typeOf(NomsType.STRUCT)) {
                int pos = 0;
                for (Map.Entry<String, NomsType> e : rowType.getFields().entrySet()) {
                    columnHandles.add(new NomsColumnHandle(connectorId, e.getKey(), pos++, e.getValue()));
                }
            } else {
                throw new PrestoException(NOT_SUPPORTED, "row type " + rowType + " non supported");
            }
            return new NomsTable(
                    new NomsTableHandle(connectorId, config.getDatabase(), schemaTableName.getTableName()),
                    columnHandles.build(),
                    URI.create(config.getDatabase() + "?ds=" + config.getDataset())
            );
        } catch (IOException e) {
            // Sloppy bail
            throw new RuntimeException(e);
        }
    }


    private KeyspaceMetadata getKeyspaceByCaseInsensitiveName(String caseInsensitiveSchemaName)
            throws SchemaNotFoundException
    {
        List<KeyspaceMetadata> keyspaces = executeWithSession(session -> session.getCluster().getMetadata().getKeyspaces());
        KeyspaceMetadata result = null;
        // Ensure that the error message is deterministic
        List<KeyspaceMetadata> sortedKeyspaces = Ordering.from(comparing(KeyspaceMetadata::getName)).immutableSortedCopy(keyspaces);
        for (KeyspaceMetadata keyspace : sortedKeyspaces) {
            if (keyspace.getName().equalsIgnoreCase(caseInsensitiveSchemaName)) {
                if (result != null) {
                    throw new PrestoException(
                            NOT_SUPPORTED,
                            format("More than one keyspace has been found for the case insensitive schema name: %s -> (%s, %s)",
                                    caseInsensitiveSchemaName, result.getName(), keyspace.getName()));
                }
                result = keyspace;
            }
        }
        if (result == null) {
            throw new SchemaNotFoundException(caseInsensitiveSchemaName);
        }
        return result;
    }

    @Override
    public ResultSet execute(String cql, Object... values)
    {
        return executeWithSession(session -> session.execute(cql, values));
    }

    @Override
    public PreparedStatement prepare(RegularStatement statement)
    {
        return executeWithSession(session -> session.prepare(statement));
    }

    @Override
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
