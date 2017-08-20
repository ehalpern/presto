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
package com.facebook.presto.noms.util;

import com.facebook.presto.noms.NomsColumnHandle;
import com.facebook.presto.noms.NomsType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.noms.util.CassandraCqlUtils.quoteStringLiteral;
import static com.facebook.presto.noms.util.CassandraCqlUtils.quoteStringLiteralForJson;
import static com.facebook.presto.noms.util.CassandraCqlUtils.validColumnName;
import static com.facebook.presto.noms.util.CassandraCqlUtils.validSchemaName;
import static com.facebook.presto.noms.util.CassandraCqlUtils.validTableName;
import static org.testng.Assert.assertEquals;

public class TestNomsCqlUtils
{
    @Test
    public void testValidSchemaName()
    {
        assertEquals("foo", validSchemaName("foo"));
        assertEquals("\"select\"", validSchemaName("select"));
    }

    @Test
    public void testValidTableName()
    {
        assertEquals("foo", validTableName("foo"));
        assertEquals("\"Foo\"", validTableName("Foo"));
        assertEquals("\"select\"", validTableName("select"));
    }

    @Test
    public void testValidColumnName()
    {
        assertEquals("foo", validColumnName("foo"));
        assertEquals("\"\"", validColumnName(CassandraCqlUtils.EMPTY_COLUMN_NAME));
        assertEquals("\"\"", validColumnName(""));
        assertEquals("\"select\"", validColumnName("select"));
    }

    @Test
    public void testQuote()
    {
        assertEquals("'foo'", quoteStringLiteral("foo"));
        assertEquals("'Presto''s'", quoteStringLiteral("Presto's"));
    }

    @Test
    public void testQuoteJson()
    {
        assertEquals("\"foo\"", quoteStringLiteralForJson("foo"));
        assertEquals("\"Presto's\"", quoteStringLiteralForJson("Presto's"));
        assertEquals("\"xx\\\"xx\"", quoteStringLiteralForJson("xx\"xx"));
    }

    @Test
    public void testAppendSelectColumns()
    {
        List<NomsColumnHandle> columns = ImmutableList.of(
                new NomsColumnHandle("", "foo", 0, NomsType.STRING),
                new NomsColumnHandle("", "bar", 0, NomsType.STRING),
                new NomsColumnHandle("", "table", 0, NomsType.STRING));

        StringBuilder sb = new StringBuilder();
        CassandraCqlUtils.appendSelectColumns(sb, columns);
        String str = sb.toString();

        assertEquals("foo,bar,\"table\"", str);
    }
}
