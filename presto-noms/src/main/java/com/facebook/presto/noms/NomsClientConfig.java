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
import com.google.common.base.Splitter;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.net.URI;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class NomsClientConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private URI ngqlURI = URI.create("http://localhost:8000/graphql");
    private String database = "noms";

    private int fetchSize = 5_000;
    private Duration clientReadTimeout = new Duration(SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS, MILLISECONDS);
    private Duration clientConnectTimeout = new Duration(SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS, MILLISECONDS);
    private Duration noHostAvailableRetryTimeout = new Duration(1, MINUTES);

    @NotNull
    public URI getNgqlURI() { return this.ngqlURI; }

    @Config("noms.ngql-uri")
    public NomsClientConfig setNgqlURI(URI uri)
    {
        this.ngqlURI = uri;
        return this;
    }

    @NotNull
    public String getDatabase() { return this.database; }

    @Config("noms.database")
    public NomsClientConfig setDatabase(String db)
    {
        this.database = db;
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
}
