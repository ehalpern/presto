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

import io.airlift.configuration.Config;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.net.URI;

public class NomsClientConfig
{
    private URI uri = URI.create("http://localhost:8000");
    private String database = "noms";
    private int fetchSize = 5_000;

    @NotNull
    public URI getURI()
    {
        return this.uri;
    }

    @Config("noms.uri")
    public NomsClientConfig setURI(URI uri)
    {
        this.uri = uri;
        return this;
    }

    @NotNull
    public String getDatabase()
    {
        return this.database;
    }

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
}
