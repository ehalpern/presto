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
package io.attic.presto.noms.ngql;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import io.airlift.log.Logger;
import io.attic.presto.noms.NomsColumnHandle;
import io.attic.presto.noms.NomsConnectorId;
import io.attic.presto.noms.NomsSession;
import io.attic.presto.noms.NomsSplit;
import io.attic.presto.noms.NomsTable;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class NgqlRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger log = Logger.get(NgqlRecordSetProvider.class);

    private final String connectorId;
    private final NomsSession nomsSession;

    @Inject
    public NgqlRecordSetProvider(NomsConnectorId connectorId, NomsSession nomsSession)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nomsSession = requireNonNull(nomsSession, "nomsSession is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        NomsSplit nomsSplit = (NomsSplit) split;

        List<NomsColumnHandle> nomsColumns = columns.stream()
                .map(column -> (NomsColumnHandle) column)
                .collect(toList());

        NomsTable table = nomsSession.getTable(nomsSplit.getTableName());
        return new NgqlRecordSet(nomsSession, nomsSplit, table, nomsColumns);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .toString();
    }
}
