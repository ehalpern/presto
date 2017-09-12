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
package io.attic.presto.noms.ngql;

import io.attic.presto.noms.NomsTable;

import javax.json.JsonException;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonValue;

import java.io.IOException;
import java.net.URI;

public class SizeQuery
        extends NomsQuery<SizeQuery.Result>
{
    public static SizeQuery create(NomsTable table)
    {
        return new SizeQuery(table);
    }

    private final String query;

    private SizeQuery(NomsTable table)
    {
        switch (table.tableType().kind()) {
            case Set:
            case List:
            case Map:
                query = "{\n" +
                        "  root {\n" +
                        "    value {\n" +
                        "      size\n" +
                        "    }\n" +
                        "  }\n" +
                        "}";
                break;
            default:
                throw new IllegalStateException("Type " + table.tableType().kind() + " not implemented");
        }
    }

    protected String query()
    {
        return query;
    }

    protected SizeQuery.Result parseResult(JsonObject json)
    {
        return new Result(json);
    }

    public Result execute(URI nomsURI, String dataset)
            throws IOException
    {
        return super.execute(nomsURI, dataset);
    }

    @Override
    public String toString()
    {
        return query;
    }

    public static class Result
            implements NomsQuery.Result
    {
        private long size;

        private Result(JsonObject json)
        {
            String fullPath = "/data/root/value/size";
            JsonValue value;
            try {
                value = json.getValue(fullPath);
                if (value.getValueType() == JsonValue.ValueType.NUMBER) {
                    size = ((JsonNumber) value).longValue();
                }
            }
            catch (JsonException e) {
                size = 0;
            }
        }

        public long size()
        {
            return size;
        }

        public String toString()
        {
            return Long.toString(size);
        }
    }
}
