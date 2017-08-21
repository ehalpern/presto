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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.facebook.presto.noms.util.*;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
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
    public List<String> getSchemaNames()
    {
        return ImmutableList.of(config.getDatabase());
    }

    @Override
    public List<String> getTableNames(String schemaName)
            throws SchemaNotFoundException
    {
        if (!config.getDatabase().equals(schemaName)) {
            throw new SchemaNotFoundException("Schema '" + schemaName + "' is not defined in configuration");
        }
        return NomsUtil.ds(config.getNgqlURI().toString());
    }

    @Override
    public NomsTable getTable(SchemaTableName schemaTableName)
            throws TableNotFoundException
    {
        ImmutableList.Builder<NomsColumnHandle> columnHandles = ImmutableList.builder();
        try {
            NgqlSchema schema = NgqlUtil.introspectQuery(config.getNgqlURI(), schemaTableName.getTableName());
            NomsType tableType = NomsType.from(schema.lastCommitValueType(), schema);
            NomsType rowType = tableType;
            if (tableType.typeOf(RootNomsType.LIST, RootNomsType.SET)) {
                // Noms collections are represented by Object<List<Struct>>>
                rowType = tableType.getTypeArguments().get(0).getTypeArguments().get(0);
            } else if (tableType.typeOf(RootNomsType.MAP)) {
                // Noms collections are represented by Object<List<Struct>>>
                rowType = tableType.getTypeArguments().get(1).getTypeArguments().get(0);
            }
            if (rowType.typeOf(RootNomsType.BLOB, RootNomsType.BOOLEAN, RootNomsType.NUMBER, RootNomsType.STRING)) {
                columnHandles.add(new NomsColumnHandle(connectorId, "value", 0, tableType));
            } else if (rowType.typeOf(RootNomsType.STRUCT)) {
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
                    URI.create(config.getDatabase() + "?ds=" + schemaTableName.getTableName())
            );
        } catch (IOException e) {
            // Sloppy bail
            throw new RuntimeException(e);
        }
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