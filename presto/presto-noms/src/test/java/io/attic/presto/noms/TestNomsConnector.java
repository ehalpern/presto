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
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;
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
public class TestNomsConnector
{
    protected static final String INVALID_DATABASE = "totally_invalid_database";
    private static final Date DATE = new Date();
    protected String database;
    protected SchemaTableName tableRowMajor;
    protected SchemaTableName table;
    protected SchemaTableName tableUnpartitioned;
    protected SchemaTableName invalidTable;
    private NomsConnector connector;
    private ConnectorMetadata metadata;
    private ConnectorSplitManager splitManager;
    private ConnectorPageSourceProvider pageSourceProvider;

    @BeforeClass
    public void setup()
            throws Exception
    {
        DatasetLoader.loadDataset("types");
        database = "test";
        //server = NomsServer.start("nbs:/tmp/presto-noms/" + database);

        String connectorId = "noms-test";
        NomsConnectorFactory connectorFactory = new NomsConnectorFactory(connectorId);

        connector = (NomsConnector) connectorFactory.create(connectorId, ImmutableMap.of(
                "noms.database-prefix", DatasetLoader.dbPefix(),
                "noms.database", DatasetLoader.dbName(),
                "noms.batch-size", "3",
                "noms.autostart", "true"),

                new TestingConnectorContext());

        metadata = connector.getMetadata(NomsTransactionHandle.INSTANCE);
        assertInstanceOf(metadata, NomsMetadata.class);

        splitManager = connector.getSplitManager();
        assertInstanceOf(splitManager, NomsSplitManager.class);

        pageSourceProvider = connector.getPageSourceProvider();
        assertInstanceOf(pageSourceProvider, NomsPageSourceProvider.class);

        tableRowMajor = new SchemaTableName(database, "types_rm");
        table = new SchemaTableName(database, "types");

        tableUnpartitioned = new SchemaTableName(database, "presto_test_unpartitioned");
        invalidTable = new SchemaTableName(database, "totally_invalid_table_name");
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        connector.shutdown();
    }

    @Test
    public void testGetDatabaseNames()
            throws Exception
    {
        List<String> databases = metadata.listSchemaNames(SESSION);
        assertTrue(databases.contains(database.toLowerCase(ENGLISH)));
    }

    @Test
    public void testGetTableNames()
            throws Exception
    {
        List<SchemaTableName> tables = metadata.listTables(SESSION, database);
        assertTrue(tables.contains(table));
    }

    // disabled until metadata manager is updated to handle invalid catalogs and schemas
    @Test(enabled = false, expectedExceptions = SchemaNotFoundException.class)
    public void testGetTableNamesException()
            throws Exception
    {
        metadata.listTables(SESSION, INVALID_DATABASE);
    }

    @Test
    public void testListUnknownSchema()
    {
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("totally_invalid_database_name", "dual")));
        assertEquals(metadata.listTables(SESSION, "totally_invalid_database_name"), ImmutableList.of());
        assertEquals(metadata.listTableColumns(SESSION, new SchemaTablePrefix("totally_invalid_database_name", "dual")), ImmutableMap.of());
    }

    @Test
    public void testGetRecordsRowMajor()
            throws Exception
    {
        testGetRecords(tableRowMajor);
    }

    @Test
    public void testGetRecordsColumnMajor()
            throws Exception
    {
        testGetRecords(table);
    }

    private void testGetRecords(SchemaTableName tableName)
            throws Exception
    {
        ConnectorTableHandle table = getTableHandle(tableName);
        ConnectorTableMetadata metadata = this.metadata.getTableMetadata(SESSION, table);
        List<ColumnHandle> columns = ImmutableList.copyOf(this.metadata.getColumnHandles(SESSION, table).values());
        Map<String, Integer> columnIndex = indexColumns(columns);
        ConnectorTransactionHandle tx = NomsTransactionHandle.INSTANCE;
        List<ConnectorTableLayoutResult> layouts = this.metadata.getTableLayouts(SESSION, table, Constraint.alwaysTrue(), Optional.empty());
        ConnectorTableLayoutHandle layout = getOnlyElement(layouts).getTableLayout().getHandle();
        List<ConnectorSplit> splits = getAllSplits(splitManager.getSplits(tx, SESSION, layout));

        long rowNumber = 0;
        for (ConnectorSplit split : splits) {
            long completedBytes = 0;
            try (ConnectorPageSource pageSource =
                    pageSourceProvider.createPageSource(tx, SESSION, split, columns)) {
                while (!pageSource.isFinished()) {
                    Page page = pageSource.getNextPage();
                    //assertPageFields(page, tableMetadata.getColumns());
                    int channelCount = page.getChannelCount();
                    int positionCount = page.getPositionCount();
                    Block[] blocks = page.getBlocks();
                    int size = blocks.length;
                    /*
                    String keyValue = cursor.getSlice(columnIndex.get("typestring")).toStringUtf8();
                    assertTrue(keyValue.startsWith("string"));
                    int rowId = Integer.parseInt(keyValue.substring(6));

                    assertEquals(keyValue, String.format("string%d", rowId));

                    assertEquals(cursor.getDouble(columnIndex.get("typedouble")), 1000.0 + rowId);

                    assertEquals(cursor.getBoolean(columnIndex.get("typebool")), rowId % 2 == 1, "rowId:" + rowId);

                    long newCompletedBytes = cursor.getCompletedBytes();
                    assertTrue(newCompletedBytes >= completedBytes);
                    completedBytes = newCompletedBytes;
                    */
                }
            }
        }
        //assertEquals(rowNumber, 6);
    }

    private static void assertReadFields(RecordCursor cursor, List<ColumnMetadata> schema)
    {
        for (int columnIndex = 0; columnIndex < schema.size(); columnIndex++) {
            ColumnMetadata column = schema.get(columnIndex);
            if (!cursor.isNull(columnIndex)) {
                Type type = column.getType();
                if (BOOLEAN.equals(type)) {
                    cursor.getBoolean(columnIndex);
                }
                else if (INTEGER.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (BIGINT.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (TIMESTAMP.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (DOUBLE.equals(type)) {
                    cursor.getDouble(columnIndex);
                }
                else if (REAL.equals(type)) {
                    cursor.getLong(columnIndex);
                }
                else if (isVarcharType(type) || VARBINARY.equals(type)) {
                    try {
                        cursor.getSlice(columnIndex);
                    }
                    catch (RuntimeException e) {
                        throw new RuntimeException("column " + column, e);
                    }
                }
                else {
                    fail("Unknown primitive type " + columnIndex);
                }
            }
        }
    }

    private ConnectorTableHandle getTableHandle(SchemaTableName tableName)
    {
        ConnectorTableHandle handle = metadata.getTableHandle(SESSION, tableName);
        checkArgument(handle != null, "rows not found: %s", tableName);
        return handle;
    }

    private static List<ConnectorSplit> getAllSplits(ConnectorSplitSource splitSource)
            throws InterruptedException
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(1000)));
        }
        return splits.build();
    }

    private static ImmutableMap<String, Integer> indexColumns(List<ColumnHandle> columnHandles)
    {
        ImmutableMap.Builder<String, Integer> index = ImmutableMap.builder();
        int i = 0;
        for (ColumnHandle columnHandle : columnHandles) {
            String name = ((NomsColumnHandle) columnHandle).name();
            index.put(name, i);
            i++;
        }
        return index.build();
    }
}
