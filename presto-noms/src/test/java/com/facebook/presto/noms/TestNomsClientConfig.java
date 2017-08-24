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
                .setURI(URI.create("http://localhost:8000"))
                .setDatabase("noms")
                .setFetchSize(5_000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("noms.uri", "http://test.com:8000")
                .put("noms.database", "testdb")
                .put("noms.fetch-size", "10000")
                .build();

        NomsClientConfig expected = new NomsClientConfig()
                .setURI(URI.create("http://test.com:8000"))
                .setDatabase("testdb")
                .setFetchSize(10_000);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
