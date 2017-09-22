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
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.attic.presto.noms.util.NomsServer;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public final class NomsQueryRunner
        extends DistributedQueryRunner
{
    public static synchronized NomsQueryRunner create(String dbName, int workerCount)
            throws Exception
    {
        NomsQueryRunner runner = new NomsQueryRunner(dbName, workerCount);

        runner.installPlugin(new NomsPlugin());
        runner.createCatalog("noms", "noms", ImmutableMap.of(
                "noms.uri", runner.noms.uri().toString(),
                "noms.autostart", "false",
                "noms.database", dbName,
                "noms.min-rows-per-split", "2",
                "noms.batch-size", "1"));

        return runner;
    }

    private final NomsServer noms;

    private NomsQueryRunner(String dbName, int workerCount)
            throws Exception
    {
        super(createSession(dbName), workerCount);
        noms = NomsServer.start(DatasetLoader.dbSpec());
    }

    private static Session createSession(String schema)
    {
        return testSessionBuilder().setCatalog("noms").setSchema(schema).build();
    }

    public void closeServer()
    {
        noms.close();
    }
}
