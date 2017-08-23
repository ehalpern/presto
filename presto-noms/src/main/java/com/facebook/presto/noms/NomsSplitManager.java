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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class NomsSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final NomsSession nomsSession;

    @Inject
    public NomsSplitManager(
            NomsConnectorId connectorId,
            NomsSession nomsSession)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nomsSession = requireNonNull(nomsSession, "nomsSession is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        NomsTableLayoutHandle layoutHandle = (NomsTableLayoutHandle) layout;
        NomsTableHandle tableHandle = layoutHandle.getTable();
        NomsTable table = nomsSession.getTable(tableHandle.getSchemaTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        return new FixedSplitSource(ImmutableList.of(
                new NomsSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(),
                        ImmutableList.of(HostAddress.fromParts(table.getSource().getHost(), table.getSource().getPort())))));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }
}
