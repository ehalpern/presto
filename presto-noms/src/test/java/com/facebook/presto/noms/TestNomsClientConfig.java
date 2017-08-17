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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SocketOptions;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestNomsClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(NomsClientConfig.class)
                .setFetchSize(5_000)
                .setConsistencyLevel(ConsistencyLevel.ONE)
                .setContactPoints("")
                .setNativeProtocolPort(9042)
                .setPartitionSizeForBatchSelect(100)
                .setSplitSize(1_024)
                .setAllowDropTable(false)
                .setUsername(null)
                .setPassword(null)
                .setClientReadTimeout(new Duration(SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS, MILLISECONDS))
                .setClientConnectTimeout(new Duration(SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS, MILLISECONDS))
                .setClientSoLinger(null)
                .setRetryPolicy(RetryPolicyType.DEFAULT)
                .setUseDCAware(false)
                .setDcAwareLocalDC(null)
                .setDcAwareUsedHostsPerRemoteDc(0)
                .setDcAwareAllowRemoteDCsForLocal(false)
                .setUseTokenAware(false)
                .setTokenAwareShuffleReplicas(false)
                .setUseWhiteList(false)
                .setWhiteListAddresses("")
                .setNoHostAvailableRetryTimeout(new Duration(1, MINUTES))
                .setSpeculativeExecutionLimit(1)
                .setSpeculativeExecutionDelay(new Duration(500, MILLISECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("noms.contact-points", "host1,host2")
                .put("noms.native-protocol-port", "9999")
                .put("noms.fetch-size", "10000")
                .put("noms.consistency-level", "TWO")
                .put("noms.partition-size-for-batch-select", "77")
                .put("noms.split-size", "1025")
                .put("noms.allow-drop-table", "true")
                .put("noms.username", "my_username")
                .put("noms.password", "my_password")
                .put("noms.client.read-timeout", "11ms")
                .put("noms.client.connect-timeout", "22ms")
                .put("noms.client.so-linger", "33")
                .put("noms.retry-policy", "BACKOFF")
                .put("noms.load-policy.use-dc-aware", "true")
                .put("noms.load-policy.dc-aware.local-dc", "dc1")
                .put("noms.load-policy.dc-aware.used-hosts-per-remote-dc", "1")
                .put("noms.load-policy.dc-aware.allow-remote-dc-for-local", "true")
                .put("noms.load-policy.use-token-aware", "true")
                .put("noms.load-policy.token-aware.shuffle-replicas", "true")
                .put("noms.load-policy.use-white-list", "true")
                .put("noms.load-policy.white-list.addresses", "host1")
                .put("noms.no-host-available-retry-timeout", "3m")
                .put("noms.speculative-execution.limit", "10")
                .put("noms.speculative-execution.delay", "101s")
                .build();

        NomsClientConfig expected = new NomsClientConfig()
                .setContactPoints("host1", "host2")
                .setNativeProtocolPort(9999)
                .setFetchSize(10_000)
                .setConsistencyLevel(ConsistencyLevel.TWO)
                .setPartitionSizeForBatchSelect(77)
                .setSplitSize(1_025)
                .setAllowDropTable(true)
                .setUsername("my_username")
                .setPassword("my_password")
                .setClientReadTimeout(new Duration(11, MILLISECONDS))
                .setClientConnectTimeout(new Duration(22, MILLISECONDS))
                .setClientSoLinger(33)
                .setRetryPolicy(RetryPolicyType.BACKOFF)
                .setUseDCAware(true)
                .setDcAwareLocalDC("dc1")
                .setDcAwareUsedHostsPerRemoteDc(1)
                .setDcAwareAllowRemoteDCsForLocal(true)
                .setUseTokenAware(true)
                .setTokenAwareShuffleReplicas(true)
                .setUseWhiteList(true)
                .setWhiteListAddresses("host1")
                .setNoHostAvailableRetryTimeout(new Duration(3, MINUTES))
                .setSpeculativeExecutionLimit(10)
                .setSpeculativeExecutionDelay(new Duration(101, SECONDS));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
