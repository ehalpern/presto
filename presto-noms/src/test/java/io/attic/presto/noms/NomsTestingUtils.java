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

import com.facebook.presto.spi.SchemaTableName;

import java.util.Date;

public class NomsTestingUtils
{
    public static final String TABLE_ALL_TYPES = "table_all_types";
    public static final String TABLE_ALL_TYPES_INSERT = "table_all_types_insert";
    public static final String TABLE_ALL_TYPES_PARTITION_KEY = "table_all_types_partition_key";
    public static final String TABLE_CLUSTERING_KEYS = "table_clustering_keys";
    public static final String TABLE_CLUSTERING_KEYS_LARGE = "table_clustering_keys_large";
    public static final String TABLE_MULTI_PARTITION_CLUSTERING_KEYS = "table_multi_partition_clustering_keys";
    public static final String TABLE_CLUSTERING_KEYS_INEQUALITY = "table_clustering_keys_inequality";

    private NomsTestingUtils() {}

    public static void createDatasets(NomsSession session, String database, Date date)
    {
        // TBD
    }

    public static void createDatabase(NomsSession session, String database)
    {
        // TBD
    }

    public static void createDatasetAllTypes(NomsSession session, SchemaTableName table, Date date, int rowsCount)
    {
        // TBD
    }
}
