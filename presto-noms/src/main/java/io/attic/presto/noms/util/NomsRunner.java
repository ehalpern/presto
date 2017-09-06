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
    /*package*/ static final String NOMS_BINARY;

    private NomsRunner() {}

    static {
        String goPath = System.getenv("GOPATH");
        if (goPath == null) {
            goPath = System.getProperty("user.home") + "/go";
        }
        NOMS_BINARY = goPath + "/bin/noms";
    }

    private static String exec(String... command)
    {
        ProcessBuilder b = new ProcessBuilder(command);
        try {
            Process p = b.start();
            String result = IOUtils.toString(p.getInputStream(), Charset.defaultCharset());
            if (!p.waitFor(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("timed out waiting for command to complete");
            }
            if (p.exitValue() != 0) {
                throw new RuntimeException("command failed: " + p.exitValue() + " :" + IOUtils.toString(p.getErrorStream(), Charset.defaultCharset()));
            }
            return result;
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> ds(String dbPath)
    {
        String result = exec(NOMS_BINARY, "ds", dbPath);
        return ImmutableList.copyOf(result.split("\n"));
    }
}
