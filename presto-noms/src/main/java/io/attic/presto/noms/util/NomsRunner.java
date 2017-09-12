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
package io.attic.presto.noms.util;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class NomsRunner
{
    public static List<String> ds(String dbSpec)
    {
        String result = exec(builder()
                .add(NOMS_BINARY)
                .add("ds")
                .add(dbSpec).build(), false);
        return ImmutableList.copyOf(result.split("\n"));
    }

    public static void deleteDS(String dsSpec)
    {
        String result = exec(builder()
                .add(NOMS_BINARY)
                .add("ds")
                .add("-d")
                .add(dsSpec).build(), true);
    }

    public static void csvImport(String csvFile, String dsSpec, String... options)
    {
        exec(builder()
                .add(CSV_IMPORT_BINARY)
                .add(options)
                .add(csvFile)
                .add(dsSpec).build(), false);
    }

    /*package*/ static final String NOMS_BINARY;
    /*package*/ static final String CSV_IMPORT_BINARY;

    private NomsRunner() {}

    static {
        String goPath = System.getenv("GOPATH");
        if (goPath == null) {
            goPath = System.getProperty("user.home") + "/go";
        }
        NOMS_BINARY = goPath + "/bin/noms";
        CSV_IMPORT_BINARY = goPath + "/bin/csv-import";
    }

    private static String exec(List<String> command, boolean ignoreErrors)
    {
        ProcessBuilder b = new ProcessBuilder(command);
        try {
            Process p = b.start();
            String result = IOUtils.toString(p.getInputStream(), Charset.defaultCharset());
            if (!p.waitFor(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("timed out waiting for command to complete");
            }
            if (!ignoreErrors && p.exitValue() != 0) {
                throw new RuntimeException("command failed: " + p.exitValue() + " :" + IOUtils.toString(p.getErrorStream(), Charset.defaultCharset()));
            }
            return result;
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static ImmutableList.Builder<String> builder()
    {
        return ImmutableList.builder();
    }
}
