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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class NomsSplitManager
        implements ConnectorSplitManager
{
    private final NomsConnectorId connectorId;
    private final NomsSession nomsSession;
    private final NodeManager nodeManager;

    @Inject
    public NomsSplitManager(
            NomsConnectorId connectorId,
            NomsSession nomsSession,
            NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.nomsSession = requireNonNull(nomsSession, "nomsSession is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    /**
     * TODO: Try splits based on primary key ranges. A naive approach is to estimate
     * the size of each row and split the key set to achieve M bytes (on average) per
     * split. A more sophisticated approach could account for prolly tree layout,
     * mapping each key range to a distinct subtree at some level of the graph to
     * minimize overlapping reads across splits.
     */
    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        NomsTableLayoutHandle layoutHandle = (NomsTableLayoutHandle) layout;
        NomsTableHandle tableHandle = layoutHandle.getTable();
        NomsTable table = nomsSession.getTable(tableHandle.getSchemaTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        TupleDomain<NomsColumnHandle> effectivePredicate = layoutHandle.getEffectivePredicate()
                .transform(NomsColumnHandle.class::cast);

        List<HostAddress> addresses = nodeManager.getWorkerNodes().stream().map(Node::getHostAndPort).collect(Collectors.toList());
        long[] lengths = computeSplitSizes();
        List<NomsSplit> splits = new ArrayList<>();
        SchemaTableName tableName = tableHandle.getSchemaTableName();
        long offset = 0;
        for (int i = 0; i < lengths.length; i++) {
            splits.add(new NomsSplit(addresses, tableName, effectivePredicate, offset, lengths[i]));
            offset += lengths[i];
        }
        return new FixedSplitSource(splits);
    }

    /**
     * Inspect the noms table to determine how to split it into minimally intersecting
     * sections. Returns an array |splits| where:
     *
     *   |splits|.size() is the number of splits
     *   |splits|[i] is the size for split i
     */
    private long[] computeSplitSizes()
    {
        //NomsSplitQuery.Result result = nomsSession.execute(NomsQuery.splitQuery());
        //return result.splitOffsets();
        // Size of last split is always 0 (no limit) to ensure no entries are missed
        // if the table has grown.
        return new long[]{20, 20, 0};
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }
}
