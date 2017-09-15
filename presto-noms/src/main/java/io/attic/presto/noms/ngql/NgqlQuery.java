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

import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import io.attic.presto.noms.NomsColumnHandle;
import io.attic.presto.noms.NomsQuery;
import io.attic.presto.noms.NomsSchema;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Form;
import org.apache.http.client.fluent.Request;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verifyNotNull;

public abstract class NgqlQuery<R extends NomsQuery.Result> implements NomsQuery<R>
{
    protected abstract String query();
    protected abstract R parseResult(JsonObject json);

    public R execute(URI nomsURI, String dataset)
            throws IOException
    {
        // TODO: replace with async IO
        Content resp = Request.Post(nomsURI.toString() + "/graphql/").bodyForm(Form.form()
                .add("ds", dataset)
                .add("query", query())
                .build())
                .execute().returnContent();

        try (JsonReader reader = Json.createReader(resp.asStream())) {
            return (R) parseResult(reader.readObject());
        }
    }

    /**
     * Build parameter list from query predicate.
     * <p>
     * Specifically, if there are constraints on the primary key column:
     * - If only exact key values are specified, use (keys: [values]) to select only those rows.
     * - If one or more non-exact key bounds (> or <) are specified, determine the range that spans
     * the full set and use (key: start, through: end) to query that range. Note that the range
     * query result will include rows corresponding to the low and high bounds if they exist, even
     * if they are not required in the range. For example, a query for keys in the range 10 < k <= 100
     * will include row 10 if it exists.
     */
    protected static List<String> paramsFromPredicate(NomsSchema schema, List<NomsColumnHandle> columns, TupleDomain<NomsColumnHandle> predicate, long offset, long limit)
    {
        List<String> params = new ArrayList<>();
        if (offset > 0) {
            params.add("at:" + offset);
        }
        if (limit > 0) {
            params.add("count:" + limit);
        }
        if (predicate.isAll() || schema.primaryKey() == null) {
            return params;
        }
        else {
            String pk = schema.primaryKey();
            return columns.stream().filter(c -> c.getName().equals(pk)).map(c -> {
                Domain constraints = predicate.getDomains().get().get(c);
                return exactValues(constraints).map(values -> {
                    params.add("keys: " + values);
                    return params;
                }).orElseGet(() ->
                        keyBounds(constraints).map(bounds -> {
                            if (bounds.getLow().getValueBlock().isPresent()) {
                                params.add("key: " + bounds.getLow().getValue());
                            }
                            if (bounds.getHigh().getValueBlock().isPresent()) {
                                params.add("through: " + bounds.getHigh().getValue());
                            }
                            return params;
                        }).get());
            }).findFirst().get();
        }
    }

    private static Optional<List<Object>> exactValues(Domain domain)
    {
        List<Object> values = new ArrayList<>();
        for (Range r : domain.getValues().getRanges().getOrderedRanges()) {
            if (r.getLow().getBound() != Marker.Bound.EXACTLY) {
                return Optional.empty();
            }
            values.add(r.getLow().getValue());
        }
        return Optional.of(values);
    }

    private static Optional<Range> keyBounds(Domain domain)
    {
        Range range = null;
        for (Range r : domain.getValues().getRanges().getOrderedRanges()) {
            if (range == null) {
                range = r;
            }
            else {
                range = range.span(r);
            }
        }
        return Optional.ofNullable(range);
    }
}
