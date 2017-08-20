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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertTrue;

public class TestNomsType
{
    @Test
    public void testJsonMapEncoding()
    {
        assertTrue(isValidJson(NomsType.buildArrayValue(Lists.newArrayList("one", "two", "three\""), NomsType.STRING)));
        assertTrue(isValidJson(NomsType.buildArrayValue(Lists.newArrayList(1, 2, 3), NomsType.NUMBER)));
        assertTrue(isValidJson(NomsType.buildArrayValue(Lists.newArrayList(100000L, 200000000L, 3000000000L), NomsType.NUMBER)));
        assertTrue(isValidJson(NomsType.buildArrayValue(Lists.newArrayList(1.0, 2.0, 3.0), NomsType.NUMBER)));
    }

    private static void continueWhileNotNull(JsonParser parser, JsonToken token)
            throws IOException
    {
        if (token != null) {
            continueWhileNotNull(parser, parser.nextToken());
        }
    }

    private static boolean isValidJson(String json)
    {
        boolean valid = false;
        try {
            JsonParser parser = new ObjectMapper().getFactory()
                    .createParser(json);
            continueWhileNotNull(parser, parser.nextToken());
            valid = true;
        }
        catch (IOException ignored) {
        }
        return valid;
    }
}
