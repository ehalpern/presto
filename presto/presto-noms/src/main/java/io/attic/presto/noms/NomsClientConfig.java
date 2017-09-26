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
    public static final URI DEFAULT_URI = URI.create("http://localhost:8000");
    public static final String DEFAULT_DATABASE_PREFIX = "nbs:/tmp/presto-noms";
    public static final String DEFAULT_DATABASE = "noms";
    public static final int DEFAULT_BATCH_SIZE = 50_000;
    public static final int DEFAULT_MIN_ROWS_PER_SPLIT = 50_000;
    public static final int DEFAULT_MAX_SPLITS_PER_NODE = 1;
    public static final boolean DEFAULT_AUTOSTART = true;

    private URI uri = DEFAULT_URI;
    private String database = DEFAULT_DATABASE;
    private String databasePrefix = DEFAULT_DATABASE_PREFIX;

    // Number of rows to request per batch. Each split will request its
    // data |batchSize| rows at a time.
    // TODO: Ideally, this would be computed based on columns in the query
    // using bytesPerBatch/estimatedBytesPerRow(columns).
    private int batchSize = DEFAULT_BATCH_SIZE;

    // Minimum number of rows per split.
    // TODO: Ideally, this would be computed by minBytesPerSplit/estimatedBytesPerRow(columns)
    private int minRowsPerSplit = DEFAULT_MIN_ROWS_PER_SPLIT;

    // Maximum number of splits per node. Defaults to 1
    private int maxSplitsPerNode = DEFAULT_MAX_SPLITS_PER_NODE;

    // Autostart noms serve |databasePrefix|/|database|
    private boolean autostart = DEFAULT_AUTOSTART;

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
    public String getDatabasePrefix()
    {
        return this.databasePrefix;
    }

    @Config("noms.database-prefix")
    public NomsClientConfig setDatabasePrefix(String prefix)
    {
        this.databasePrefix = prefix;
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
    public int getBatchSize()
    {
        return batchSize;
    }

    @Config("noms.batch-size")
    public NomsClientConfig setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    @Min(1)
    public int getMinRowsPerSplit()
    {
        return minRowsPerSplit;
    }

    @Config("noms.min-rows-per-split")
    public NomsClientConfig setMinRowsPerSplit(int minRows)
    {
        this.minRowsPerSplit = minRows;
        return this;
    }

    @Min(0)
    public int getMaxSplitsPerNode()
    {
        return maxSplitsPerNode;
    }

    @Config("noms.max-splits-per-node")
    public NomsClientConfig setMaxSplitsPerNode(int maxSplits)
    {
        this.maxSplitsPerNode = maxSplits;
        return this;
    }

    public boolean isAutostart()
    {
        return autostart;
    }

    @Config("noms.autostart")
    public NomsClientConfig setAutostart(boolean autostart)
    {
        this.autostart = autostart;
        return this;
    }
}
