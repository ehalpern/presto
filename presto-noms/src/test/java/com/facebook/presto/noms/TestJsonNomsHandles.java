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

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestJsonNomsHandles
{
    private static final Map<String, Object> TABLE_HANDLE_AS_MAP = ImmutableMap.of(
            "connectorId", "noms",
            "schemaName", "noms_schema",
            "tableName", "noms_table");

    private static final Map<String, Object> COLUMN_HANDLE_AS_MAP = ImmutableMap.<String, Object>builder()
            .put("connectorId", "noms")
            .put("name", "column")
            .put("ordinalPosition", 42)
            .put("nomsType", "BIGINT")
            .put("partitionKey", false)
            .put("clusteringKey", true)
            .put("indexed", false)
            .put("hidden", false)
            .build();

    private static final Map<String, Object> COLUMN2_HANDLE_AS_MAP = ImmutableMap.<String, Object>builder()
            .put("connectorId", "noms")
            .put("name", "column2")
            .put("ordinalPosition", 0)
            .put("nomsType", "SET")
            .put("typeArguments", ImmutableList.of("INT"))
            .put("partitionKey", false)
            .put("clusteringKey", false)
            .put("indexed", false)
            .put("hidden", false)
            .build();

    private final ObjectMapper objectMapper = new ObjectMapperProvider().get();

    @Test
    public void testTableHandleSerialize()
            throws Exception
    {
        NomsTableHandle tableHandle = new NomsTableHandle("noms", "noms_schema", "noms_table");

        assertTrue(objectMapper.canSerialize(NomsTableHandle.class));
        String json = objectMapper.writeValueAsString(tableHandle);
        testJsonEquals(json, TABLE_HANDLE_AS_MAP);
    }

    @Test
    public void testTableHandleDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(TABLE_HANDLE_AS_MAP);

        NomsTableHandle tableHandle = objectMapper.readValue(json, NomsTableHandle.class);

        assertEquals(tableHandle.getConnectorId(), "noms");
        assertEquals(tableHandle.getSchemaName(), "noms_schema");
        assertEquals(tableHandle.getTableName(), "noms_table");
        assertEquals(tableHandle.getSchemaTableName(), new SchemaTableName("noms_schema", "noms_table"));
    }

    @Test
    public void testColumnHandleSerialize()
            throws Exception
    {
        NomsColumnHandle columnHandle = new NomsColumnHandle("noms", "column", 42, NomsType.BIGINT, null, false, true, false, false);

        assertTrue(objectMapper.canSerialize(NomsColumnHandle.class));
        String json = objectMapper.writeValueAsString(columnHandle);
        testJsonEquals(json, COLUMN_HANDLE_AS_MAP);
    }

    @Test
    public void testColumn2HandleSerialize()
            throws Exception
    {
        NomsColumnHandle columnHandle = new NomsColumnHandle(
                "noms",
                "column2",
                0,
                NomsType.SET,
                ImmutableList.of(NomsType.INT),
                false,
                false,
                false,
                false);

        assertTrue(objectMapper.canSerialize(NomsColumnHandle.class));
        String json = objectMapper.writeValueAsString(columnHandle);
        testJsonEquals(json, COLUMN2_HANDLE_AS_MAP);
    }

    @Test
    public void testColumnHandleDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(COLUMN_HANDLE_AS_MAP);

        NomsColumnHandle columnHandle = objectMapper.readValue(json, NomsColumnHandle.class);

        assertEquals(columnHandle.getName(), "column");
        assertEquals(columnHandle.getOrdinalPosition(), 42);
        assertEquals(columnHandle.getNomsType(), NomsType.BIGINT);
        assertEquals(columnHandle.getTypeArguments(), null);
        assertEquals(columnHandle.isPartitionKey(), false);
        assertEquals(columnHandle.isClusteringKey(), true);
    }

    @Test
    public void testColumn2HandleDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(COLUMN2_HANDLE_AS_MAP);

        NomsColumnHandle columnHandle = objectMapper.readValue(json, NomsColumnHandle.class);

        assertEquals(columnHandle.getName(), "column2");
        assertEquals(columnHandle.getOrdinalPosition(), 0);
        assertEquals(columnHandle.getNomsType(), NomsType.SET);
        assertEquals(columnHandle.getTypeArguments(), ImmutableList.of(NomsType.INT));
        assertEquals(columnHandle.isPartitionKey(), false);
        assertEquals(columnHandle.isClusteringKey(), false);
    }

    private void testJsonEquals(String json, Map<String, Object> expectedMap)
            throws Exception
    {
        Map<String, Object> jsonMap = objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        assertEqualsIgnoreOrder(jsonMap.entrySet(), expectedMap.entrySet());
    }
}
