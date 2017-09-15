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
import io.attic.presto.noms.NomsQuery;
import io.attic.presto.noms.NomsSchema;
import io.attic.presto.noms.NomsTable;

import javax.json.JsonObject;

import java.util.Collections;
import java.util.List;

public class SizeQuery
        extends NgqlQuery<SizeQuery.Result>
{
    public static SizeQuery create(NomsTable table)
    {
        return new SizeQuery(table);
    }

    private final NgqlQuery query;

    private SizeQuery(NomsTable table)
    {
        List<NomsColumnHandle> noColumns = Collections.emptyList();
        TupleDomain<NomsColumnHandle> noPredicates = TupleDomain.none();
        NomsSchema schema = table.schema();
        switch (schema.tableStructure()) {
            case ColumnMajor:
                query = ColumnQuery.create(schema, noColumns, noPredicates, 0, 0);
                break;
            case RowMajor:
                query = RowQuery.create(schema, noColumns, noPredicates, 0, 0);
                break;
            default:
                throw new AssertionError("Unsupported table structure " + schema.tableStructure());
        }
    }

    protected String query()
    {
        return query.query();
    }

    protected SizeQuery.Result parseResult(JsonObject json)
    {
        return new Result(query.parseResult(json).size());
    }

    @Override
    public String toString()
    {
        return query.query();
    }

    public static class Result
            implements NomsQuery.Result
    {
        private int size;

        private Result(int size)
        {
            this.size = size;
        }

        public int size()
        {
            return size;
        }

        public String toString()
        {
            return Integer.toString(size);
        }
    }
}
