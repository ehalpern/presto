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

import com.datastax.driver.core.SocketOptions;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestNomsClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(NomsClientConfig.class)
                .setNgqlURI(URI.create("http://localhost:8000/graphql"))
                .setDatabase("noms")
                .setDataset(null)
                .setFetchSize(5_000)
                .setClientReadTimeout(new Duration(SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS, MILLISECONDS))
                .setClientConnectTimeout(new Duration(SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS, MILLISECONDS))
                .setNoHostAvailableRetryTimeout(new Duration(1, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("noms.ngql-uri", "http://test.com:8000/graphql")
                .put("noms.database", "test-db")
                .put("noms.dataset", "test-ds")
                .put("noms.fetch-size", "10000")
                .put("noms.client.read-timeout", "11ms")
                .put("noms.client.connect-timeout", "22ms")
                .put("noms.no-host-available-retry-timeout", "3m")
                .build();

        NomsClientConfig expected = new NomsClientConfig()
                .setNgqlURI(URI.create("http://test.com:8000/graphql"))
                .setDatabase("test-db")
                .setDataset("test-ds")
                .setFetchSize(10_000)
                .setClientReadTimeout(new Duration(11, MILLISECONDS))
                .setClientConnectTimeout(new Duration(22, MILLISECONDS))
                .setNoHostAvailableRetryTimeout(new Duration(3, MINUTES));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
