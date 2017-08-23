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
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Collections;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestNomsColumnHandle
{
    private final JsonCodec<NomsColumnHandle> codec = jsonCodec(NomsColumnHandle.class);

    @Test
    public void testRoundTrip()
    {
        NomsColumnHandle[] columns = {
                new NomsColumnHandle("cid", "c0", 0, RootNomsType.Number),
                new NomsColumnHandle("cid", "c1", 1, DerivedNomsType.EMPTY_LIST),
                new NomsColumnHandle("cid", "c2", 2, new DerivedNomsType(
                        "NumberList", RootNomsType.List, ImmutableList.of(RootNomsType.Number))),
                new NomsColumnHandle("cid", "c3", 3, new DerivedNomsType(
                        "TestStruct", RootNomsType.Struct, Collections.EMPTY_LIST, ImmutableMap.of("test", RootNomsType.String)))
        };

        for (int i = 0; i < columns.length; i++) {
            NomsColumnHandle expected = columns[i];
            String json = codec.toJson(expected);
            NomsColumnHandle actual = codec.fromJson(json);
            assertEquals(actual.getConnectorId(), expected.getConnectorId(), "column: " + expected.getName());
            assertEquals(actual.getName(), expected.getName());
            assertEquals(actual.getOrdinalPosition(), expected.getOrdinalPosition());
            assertEquals(actual.getNomsType(), expected.getNomsType());
        }
    }
}
