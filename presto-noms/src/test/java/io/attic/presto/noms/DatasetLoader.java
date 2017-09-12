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

import io.attic.presto.noms.util.NomsRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DatasetLoader
{
    private static final Path TEST_DB_PATH = Paths.get("/tmp", "presto-noms", "test");
    private static final String TEST_DATA = "test-data";

    public static String loadDataset(String dsName)
    {
        try {
            String csvFile = filePath(String.format("%s/%s.csv", TEST_DATA, dsName));
            String columnTypesOption = columnTypesOption(dsName);
            String dsSpec = String.format("%s::%s", findOrCreateDB(), dsName);
            NomsRunner.deleteDS(dsSpec);
            NomsRunner.csvImport(csvFile, dsSpec, columnTypesOption);
            return dsSpec;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String columnTypesOption(String dsName)
    {
        String file = "";
        try {
            file = filePath(String.format("%s/%s.types", TEST_DATA, dsName));
            String types = Files.readAllLines(Paths.get(file), Charset.defaultCharset()).get(0);
            return "--column-types=" + types;
        }
        catch (FileNotFoundException e) {
            return "";
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read " + file, e);
        }
    }

    private static String filePath(String resource)
            throws FileNotFoundException
    {
        URL url = DatasetLoader.class.getClassLoader().getResource(resource);
        if (url == null) {
            throw new RuntimeException(String.format("Resource '%s' not found", resource));
        }
        return url.getPath();
    }

    private static String findOrCreateDB()
            throws IOException
    {
        try {
            Files.createDirectories(TEST_DB_PATH);
            return "nbs:" + TEST_DB_PATH.toString();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DatasetLoader() {}
}
