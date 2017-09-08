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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

public class TestNomsClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(NomsClientConfig.class)
                .setURI(NomsClientConfig.DEFAULT_URI)
                .setDatabase(NomsClientConfig.DEFAULT_DATABASE)
                .setBatchSize(NomsClientConfig.DEFAULT_BATCH_SIZE)
                .setMinRowsPerSplit(NomsClientConfig.DEFAULT_MIN_ROWS_PER_SPLIT)
                .setMaxSplitsPerNode(NomsClientConfig.DEFAULT_MAX_SPLITS_PER_NODE));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("noms.uri", "http://test.com:8000")
                .put("noms.database", "testdb")
                .put("noms.batch-size", "10000")
                .put("noms.min-rows-per-split", "10000")
                .put("noms.max-splits-per-node", "5")
                .build();

        NomsClientConfig expected = new NomsClientConfig()
                .setURI(URI.create("http://test.com:8000"))
                .setDatabase("testdb")
                .setBatchSize(10_000)
                .setMinRowsPerSplit(10_000)
                .setMaxSplitsPerNode(5);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
