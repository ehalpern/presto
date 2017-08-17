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

import com.datastax.driver.core.VersionNumber;
import com.facebook.presto.noms.*;
import com.facebook.presto.noms.NomsType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestNomsClusteringPredicatesExtractor
{
    private static NomsColumnHandle col1;
    private static NomsColumnHandle col2;
    private static NomsColumnHandle col3;
    private static NomsColumnHandle col4;
    private static NomsTable nomsTable;
    private static VersionNumber cassandraVersion;

    @BeforeTest
    void setUp()
            throws Exception
    {
        col1 = new NomsColumnHandle("noms", "partitionKey1", 1, NomsType.BIGINT, null, true, false, false, false);
        col2 = new NomsColumnHandle("noms", "clusteringKey1", 2, NomsType.BIGINT, null, false, true, false, false);
        col3 = new NomsColumnHandle("noms", "clusteringKey2", 3, NomsType.BIGINT, null, false, true, false, false);
        col4 = new NomsColumnHandle("noms", "clusteringKe3", 4, NomsType.BIGINT, null, false, true, false, false);

        nomsTable = new NomsTable(
                new NomsTableHandle("noms", "test", "records"), ImmutableList.of(col1, col2, col3, col4));

        cassandraVersion = VersionNumber.parse("2.1.5");
    }

    @Test
    public void testBuildClusteringPredicate()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        col1, Domain.singleValue(BIGINT, 23L),
                        col2, Domain.singleValue(BIGINT, 34L),
                        col4, Domain.singleValue(BIGINT, 26L)));
        NomsClusteringPredicatesExtractor predicatesExtractor = new NomsClusteringPredicatesExtractor(nomsTable.getClusteringKeyColumns(), tupleDomain, cassandraVersion);
        String predicate = predicatesExtractor.getClusteringKeyPredicates();
        assertEquals(predicate, new StringBuilder("\"clusteringKey1\" = 34").toString());
    }

    @Test
    public void testGetUnenforcedPredicates()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        col2, Domain.singleValue(BIGINT, 34L),
                        col4, Domain.singleValue(BIGINT, 26L)));
        NomsClusteringPredicatesExtractor predicatesExtractor = new NomsClusteringPredicatesExtractor(nomsTable.getClusteringKeyColumns(), tupleDomain, cassandraVersion);
        TupleDomain<ColumnHandle> unenforcedPredicates = TupleDomain.withColumnDomains(ImmutableMap.of(col4, Domain.singleValue(BIGINT, 26L)));
        assertEquals(predicatesExtractor.getUnenforcedConstraints(), unenforcedPredicates);
    }
}
