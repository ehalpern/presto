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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;

import java.util.List;

public interface NomsSession
{
    String PRESTO_COMMENT_METADATA = "Presto Metadata:";

    List<String> getSchemaNames();

    List<String> getTableNames(String schemaName)
            throws SchemaNotFoundException;

    NomsTable getTable(SchemaTableName schemaTableName)
            throws TableNotFoundException;

    // NomsResultSet execute(NomsQuery query);
    ResultSet execute(String cql, Object... values);

    PreparedStatement prepare(RegularStatement statement);

    ResultSet execute(Statement statement);
}
