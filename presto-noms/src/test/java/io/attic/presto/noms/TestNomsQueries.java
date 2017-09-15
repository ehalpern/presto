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
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.attic.presto.noms.ngql.SizeQuery;
import io.attic.presto.noms.util.NomsServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.xml.crypto.Data;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestNomsQueries
{
    protected String database;
    protected SchemaTableName rowTable;
    protected SchemaTableName columnTable;
    private NomsMetadata metadata;
    private NomsServer server;
    private NomsSession session;

    @BeforeClass
    public void setup()
            throws Exception
    {
        DatasetLoader.loadDataset("types");
        server = NomsServer.start(DatasetLoader.dbSpec());
        database = DatasetLoader.dbName();

        String connectorId = "noms-test";
        NomsConnectorFactory connectorFactory = new NomsConnectorFactory(connectorId);
        NomsConnector connector = (NomsConnector) connectorFactory.create(connectorId, ImmutableMap.of(
                "noms.uri", server.uri().toString(),
                "noms.database", "test"),
                new TestingConnectorContext());

        metadata = (NomsMetadata)connector.getMetadata(NomsTransactionHandle.INSTANCE);
        session = ((NomsMetadata)metadata).session();

        rowTable = new SchemaTableName(database, "types_rm");
        columnTable = new SchemaTableName(database, "types");
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        server.close();
    }

    @Test
    public void testSizeQuery()
            throws Exception
    {
        NomsTable table = session.getTable(rowTable);
        SizeQuery.Result result = session.execute(rowTable.getTableName(), SizeQuery.create(table));
        assertEquals(6, result.size());

        table = session.getTable(columnTable);
        result = session.execute(columnTable.getTableName(), SizeQuery.create(table));
        assertEquals(6, result.size());
    }
}
