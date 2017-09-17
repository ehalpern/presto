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

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import io.attic.presto.noms.ngql.SizeQuery;
import io.attic.presto.noms.util.NomsServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

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

        metadata = (NomsMetadata) connector.getMetadata(NomsTransactionHandle.INSTANCE);
        session = ((NomsMetadata) metadata).session();

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
