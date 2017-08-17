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

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestNomsColumnHandle
{
    private final JsonCodec<NomsColumnHandle> codec = jsonCodec(NomsColumnHandle.class);

    @Test
    public void testRoundTrip()
    {
        NomsColumnHandle expected = new NomsColumnHandle("connector", "name", 42, NomsType.FLOAT, null, true, false, false, false);

        String json = codec.toJson(expected);
        NomsColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOrdinalPosition(), expected.getOrdinalPosition());
        assertEquals(actual.getNomsType(), expected.getNomsType());
        assertEquals(actual.isPartitionKey(), expected.isPartitionKey());
        assertEquals(actual.isClusteringKey(), expected.isClusteringKey());
    }

    @Test
    public void testRoundTrip2()
    {
        NomsColumnHandle expected = new NomsColumnHandle(
                "connector",
                "name2",
                1,
                NomsType.MAP,
                ImmutableList.of(NomsType.VARCHAR, NomsType.UUID),
                false,
                true,
                false,
                false);

        String json = codec.toJson(expected);
        NomsColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOrdinalPosition(), expected.getOrdinalPosition());
        assertEquals(actual.getNomsType(), expected.getNomsType());
        assertEquals(actual.isPartitionKey(), expected.isPartitionKey());
        assertEquals(actual.isClusteringKey(), expected.isClusteringKey());
    }
}
