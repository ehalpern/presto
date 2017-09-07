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

import com.facebook.presto.spi.predicate.TupleDomain;
import io.attic.presto.noms.NomsColumnHandle;
import io.attic.presto.noms.NomsTable;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Form;
import org.apache.http.client.fluent.Request;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class Ngql
{
    public interface Query<R extends Ngql.Result>
    {
        String query();
        R newResult(JsonObject json);
    }

    public interface Result<T extends Ngql.Result> {

    }

    public static SchemaQuery schemaQuery()
    {
        return new SchemaQuery();
    }

    public static RowQuery rowQuery(
            NomsTable table,
            List<NomsColumnHandle> columns,
            TupleDomain<NomsColumnHandle> predicate,
            long offset,
            long limit)
    {
        return new RowQuery(table, columns, predicate, offset, limit);
    }

    /*
    public static SplitQuery rowQuery(
            NomsTable table,
            List<NomsColumnHandle> columns,
            TupleDomain<NomsColumnHandle> predicate,
    {

    }
    */

    public static <Q extends Ngql.Query<R>, R extends Ngql.Result> R execute(
            URI nomsURI,
            String dataset,
            Q query)
            throws IOException
    {
        Content resp = Request.Post(nomsURI.toString() + "/graphql/").bodyForm(Form.form()
                .add("ds", dataset)
                .add("query", query.query())
                .build())
                .execute().returnContent();

        try (JsonReader reader = Json.createReader(resp.asStream())) {
            return (R)query.newResult(reader.readObject());
        }
    }



}
