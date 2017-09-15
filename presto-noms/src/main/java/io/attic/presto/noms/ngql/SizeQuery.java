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

import io.attic.presto.noms.NomsQuery;
import io.attic.presto.noms.NomsSchema;
import io.attic.presto.noms.NomsTable;
import io.attic.presto.noms.NomsType;

import javax.json.JsonException;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonValue;

import java.io.IOException;
import java.net.URI;

public class SizeQuery
        extends NgqlQuery<SizeQuery.Result>
{
    public static SizeQuery create(NomsTable table)
    {
        return new SizeQuery(table);
    }

    private final String query;
    private final String sizePath;

    private SizeQuery(NomsTable table)
    {
        NomsSchema schema = table.schema();
        NomsType.Kind kind = schema.tableType().kind();
        switch (kind) {
            case Set:
            case List:
            case Map:
                query = "{ root {\n" +
                        "    value { size }\n" +
                        "}}";
                sizePath = "root/value/size";
                break;
            case Struct:
                String col = schema.columns().get(0).getKey();
                query = "{ root { value {\n" +
                        "  " + col + "{ size } }\n" +
                        "}}}";
                sizePath = "root/value/" + col + "/size";
                break;

            default:
                throw new IllegalStateException("Type " + kind + " not implemented");
        }
    }

    protected String query()
    {
        return query;
    }

    protected SizeQuery.Result parseResult(JsonObject json)
    {
        return new Result(json, sizePath);
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

        private Result(JsonObject json, String sizePath)
        {
            String fullPath = "/data/" + sizePath;
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
