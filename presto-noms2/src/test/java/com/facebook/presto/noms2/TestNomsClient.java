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
package com.facebook.presto.noms2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.presto.noms2.MetadataUtil.CATALOG_CODEC;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestNomsClient
{
    @Test
    public void testMetadata()
            throws Exception
    {
        URI metadata = Resources.getResource(TestNomsClient.class, "/example-data/example-metadata.json").toURI();
        URI ngqlURI = URI.create("http://localhost:8000/graphql");
        NomsConfig cfg = new NomsConfig().
                setMetadata(metadata).
                setNgqlURI(ngqlURI);
        NomsClient client = new NomsClient(cfg, CATALOG_CODEC);
        assertEquals(client.getSchemaNames(), ImmutableSet.of("example", "tpch"));
        assertEquals(client.getTableNames("example"), ImmutableSet.of("numbers"));
        assertEquals(client.getTableNames("tpch"), ImmutableSet.of("orders", "lineitem"));

        NomsTable table = client.getTable("example", "numbers");
        assertNotNull(table, "table is null");
        assertEquals(table.getName(), "numbers");
        assertEquals(table.getColumns(), ImmutableList.of(new NomsColumn("text", createUnboundedVarcharType()), new NomsColumn("value", BIGINT)));
        assertEquals(table.getSources(), ImmutableList.of(metadata.resolve("numbers-1.csv"), metadata.resolve("numbers-2.csv")));
    }
}