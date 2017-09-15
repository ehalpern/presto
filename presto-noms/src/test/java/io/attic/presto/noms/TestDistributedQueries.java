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

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestDistributedQueries
        extends AbstractTestQueryFramework
{
    public TestDistributedQueries()
    {
        super(() -> NomsQueryRunner.create("test", 3));
    }

    @AfterClass
    public void tearDown()
    {
        ((NomsQueryRunner) getQueryRunner()).closeServer();
    }

    @Test
    public void testSimpleSelect()
    {
        assertQuery(
                "SELECT typestring, typebool, typedouble from types",
                new Object[][] {
                        {"string0", false, 1000},
                        {"string1", true, 1001},
                        {"string2", false, 1002},
                        {"string3", true, 1003},
                        {"string4", false, 1004},
                        {"string5", true, 1005}
                });
    }

    @Test
    public void testSimpleSelectRowMajor()
    {
        assertQuery(
                "SELECT typestring, typebool, typedouble from types_rm",
                new Object[][] {
                        {"string0", false, 1000},
                        {"string1", true, 1001},
                        {"string2", false, 1002},
                        {"string3", true, 1003},
                        {"string4", false, 1004},
                        {"string5", true, 1005}
                });
    }

    private void assertQuery(String sql, Object[][] expected)
    {
        assertQuery(getSession(), sql, expectedResult(expected));
    }

    private List<MaterializedRow> expectedResult(Object[][] expected)
    {
        return Arrays.stream(expected).map(
                row -> new MaterializedRow(64,
                        Arrays.stream(row).map(o ->
                                o instanceof Number ? ((Number) o).doubleValue() : o
                        ).collect(Collectors.toList()))
        ).collect(Collectors.toList());
    }

    private void assertQuery(
            Session session,
            String sql,
            List<MaterializedRow> expectedRows)
    {
        long start = System.nanoTime();
        MaterializedResult actualResults = null;
        try {
            actualResults = computeActual(session, sql);
        }
        catch (RuntimeException ex) {
            fail("query failed: " + sql, ex);
        }
        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        assertEqualsIgnoreOrder(actualRows, expectedRows, "For query: \n " + sql);
    }
}
