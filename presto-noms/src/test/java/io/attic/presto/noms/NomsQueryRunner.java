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
{
    private NomsQueryRunner()
    {
    }

    public static synchronized DistributedQueryRunner create()
            throws Exception
    {
        NomsServer noms = NomsServer.start("nbs:/tmp/presto-noms/tpch");

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createNomsSession("tpch"), 4);

        queryRunner.installPlugin(new NomsPlugin());
        queryRunner.createCatalog("noms", "noms", ImmutableMap.of(
                "noms.uri", noms.uri().toString(),
                "noms.database", "tpch"));

        return queryRunner;
    }

    public static Session createNomsSession(String schema)
    {
        return testSessionBuilder()
                .setCatalog("noms")
                .setSchema(schema)
                .build();
    }
}
