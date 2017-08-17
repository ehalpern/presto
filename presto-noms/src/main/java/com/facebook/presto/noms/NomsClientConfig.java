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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.util.Arrays;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig({"noms.thrift-port", "noms.partitioner", "noms.thrift-connection-factory-class", "noms.transport-factory-options",
        "noms.no-host-available-retry-count", "noms.max-schema-refresh-threads", "noms.schema-cache-ttl",
        "noms.schema-refresh-interval"})
public class NomsClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
    private int fetchSize = 5_000;
    private List<String> contactPoints = ImmutableList.of();
    private int nativeProtocolPort = 9042;
    private int partitionSizeForBatchSelect = 100;
    private int splitSize = 1_024;
    private boolean allowDropTable;
    private String username;
    private String password;
    private Duration clientReadTimeout = new Duration(SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS, MILLISECONDS);
    private Duration clientConnectTimeout = new Duration(SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS, MILLISECONDS);
    private Integer clientSoLinger;
    private RetryPolicyType retryPolicy = RetryPolicyType.DEFAULT;
    private boolean useDCAware;
    private String dcAwareLocalDC;
    private int dcAwareUsedHostsPerRemoteDc;
    private boolean dcAwareAllowRemoteDCsForLocal;
    private boolean useTokenAware;
    private boolean tokenAwareShuffleReplicas;
    private boolean useWhiteList;
    private List<String> whiteListAddresses = ImmutableList.of();
    private Duration noHostAvailableRetryTimeout = new Duration(1, MINUTES);
    private int speculativeExecutionLimit = 1;
    private Duration speculativeExecutionDelay = new Duration(500, MILLISECONDS);

    @NotNull
    @Size(min = 1)
    public List<String> getContactPoints()
    {
        return contactPoints;
    }

    @Config("noms.contact-points")
    public NomsClientConfig setContactPoints(String commaSeparatedList)
    {
        this.contactPoints = SPLITTER.splitToList(commaSeparatedList);
        return this;
    }

    public NomsClientConfig setContactPoints(String... contactPoints)
    {
        this.contactPoints = Arrays.asList(contactPoints);
        return this;
    }

    @Min(1)
    public int getNativeProtocolPort()
    {
        return nativeProtocolPort;
    }

    @Config(("noms.native-protocol-port"))
    public NomsClientConfig setNativeProtocolPort(int nativeProtocolPort)
    {
        this.nativeProtocolPort = nativeProtocolPort;
        return this;
    }

    @NotNull
    public ConsistencyLevel getConsistencyLevel()
    {
        return consistencyLevel;
    }

    @Config("noms.consistency-level")
    public NomsClientConfig setConsistencyLevel(ConsistencyLevel level)
    {
        this.consistencyLevel = level;
        return this;
    }

    @Min(1)
    public int getFetchSize()
    {
        return fetchSize;
    }

    @Config("noms.fetch-size")
    public NomsClientConfig setFetchSize(int fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }

    @Min(1)
    public int getPartitionSizeForBatchSelect()
    {
        return partitionSizeForBatchSelect;
    }

    @Config("noms.partition-size-for-batch-select")
    public NomsClientConfig setPartitionSizeForBatchSelect(int partitionSizeForBatchSelect)
    {
        this.partitionSizeForBatchSelect = partitionSizeForBatchSelect;
        return this;
    }

    @Min(1)
    public int getSplitSize()
    {
        return splitSize;
    }

    @Config("noms.split-size")
    public NomsClientConfig setSplitSize(int splitSize)
    {
        this.splitSize = splitSize;
        return this;
    }

    public boolean getAllowDropTable()
    {
        return this.allowDropTable;
    }

    @Config("noms.allow-drop-table")
    @ConfigDescription("Allow hive connector to drop table")
    public NomsClientConfig setAllowDropTable(boolean allowDropTable)
    {
        this.allowDropTable = allowDropTable;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("noms.username")
    public NomsClientConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("noms.password")
    @ConfigSecuritySensitive
    public NomsClientConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("1h")
    public Duration getClientReadTimeout()
    {
        return clientReadTimeout;
    }

    @Config("noms.client.read-timeout")
    public NomsClientConfig setClientReadTimeout(Duration clientReadTimeout)
    {
        this.clientReadTimeout = clientReadTimeout;
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("1h")
    public Duration getClientConnectTimeout()
    {
        return clientConnectTimeout;
    }

    @Config("noms.client.connect-timeout")
    public NomsClientConfig setClientConnectTimeout(Duration clientConnectTimeout)
    {
        this.clientConnectTimeout = clientConnectTimeout;
        return this;
    }

    @Min(0)
    public Integer getClientSoLinger()
    {
        return clientSoLinger;
    }

    @Config("noms.client.so-linger")
    public NomsClientConfig setClientSoLinger(Integer clientSoLinger)
    {
        this.clientSoLinger = clientSoLinger;
        return this;
    }

    @NotNull
    public RetryPolicyType getRetryPolicy()
    {
        return retryPolicy;
    }

    @Config("noms.retry-policy")
    public NomsClientConfig setRetryPolicy(RetryPolicyType retryPolicy)
    {
        this.retryPolicy = retryPolicy;
        return this;
    }

    public boolean isUseDCAware()
    {
        return this.useDCAware;
    }

    @Config("noms.load-policy.use-dc-aware")
    public NomsClientConfig setUseDCAware(boolean useDCAware)
    {
        this.useDCAware = useDCAware;
        return this;
    }

    public String getDcAwareLocalDC()
    {
        return dcAwareLocalDC;
    }

    @Config("noms.load-policy.dc-aware.local-dc")
    public NomsClientConfig setDcAwareLocalDC(String dcAwareLocalDC)
    {
        this.dcAwareLocalDC = dcAwareLocalDC;
        return this;
    }

    @Min(0)
    public Integer getDcAwareUsedHostsPerRemoteDc()
    {
        return dcAwareUsedHostsPerRemoteDc;
    }

    @Config("noms.load-policy.dc-aware.used-hosts-per-remote-dc")
    public NomsClientConfig setDcAwareUsedHostsPerRemoteDc(Integer dcAwareUsedHostsPerRemoteDc)
    {
        this.dcAwareUsedHostsPerRemoteDc = dcAwareUsedHostsPerRemoteDc;
        return this;
    }

    public boolean isDcAwareAllowRemoteDCsForLocal()
    {
        return this.dcAwareAllowRemoteDCsForLocal;
    }

    @Config("noms.load-policy.dc-aware.allow-remote-dc-for-local")
    public NomsClientConfig setDcAwareAllowRemoteDCsForLocal(boolean dcAwareAllowRemoteDCsForLocal)
    {
        this.dcAwareAllowRemoteDCsForLocal = dcAwareAllowRemoteDCsForLocal;
        return this;
    }

    public boolean isUseTokenAware()
    {
        return this.useTokenAware;
    }

    @Config("noms.load-policy.use-token-aware")
    public NomsClientConfig setUseTokenAware(boolean useTokenAware)
    {
        this.useTokenAware = useTokenAware;
        return this;
    }

    public boolean isTokenAwareShuffleReplicas()
    {
        return this.tokenAwareShuffleReplicas;
    }

    @Config("noms.load-policy.token-aware.shuffle-replicas")
    public NomsClientConfig setTokenAwareShuffleReplicas(boolean tokenAwareShuffleReplicas)
    {
        this.tokenAwareShuffleReplicas = tokenAwareShuffleReplicas;
        return this;
    }

    public boolean isUseWhiteList()
    {
        return this.useWhiteList;
    }

    @Config("noms.load-policy.use-white-list")
    public NomsClientConfig setUseWhiteList(boolean useWhiteList)
    {
        this.useWhiteList = useWhiteList;
        return this;
    }

    public List<String> getWhiteListAddresses()
    {
        return whiteListAddresses;
    }

    @Config("noms.load-policy.white-list.addresses")
    public NomsClientConfig setWhiteListAddresses(String commaSeparatedList)
    {
        this.whiteListAddresses = SPLITTER.splitToList(commaSeparatedList);
        return this;
    }

    @NotNull
    public Duration getNoHostAvailableRetryTimeout()
    {
        return noHostAvailableRetryTimeout;
    }

    @Config("noms.no-host-available-retry-timeout")
    public NomsClientConfig setNoHostAvailableRetryTimeout(Duration noHostAvailableRetryTimeout)
    {
        this.noHostAvailableRetryTimeout = noHostAvailableRetryTimeout;
        return this;
    }

    @Min(1)
    public int getSpeculativeExecutionLimit()
    {
        return speculativeExecutionLimit;
    }

    @Config("noms.speculative-execution.limit")
    public NomsClientConfig setSpeculativeExecutionLimit(int speculativeExecutionLimit)
    {
        this.speculativeExecutionLimit = speculativeExecutionLimit;
        return this;
    }

    @MinDuration("1ms")
    public Duration getSpeculativeExecutionDelay()
    {
        return speculativeExecutionDelay;
    }

    @Config("noms.speculative-execution.delay")
    public NomsClientConfig setSpeculativeExecutionDelay(Duration speculativeExecutionDelay)
    {
        this.speculativeExecutionDelay = speculativeExecutionDelay;
        return this;
    }
}
