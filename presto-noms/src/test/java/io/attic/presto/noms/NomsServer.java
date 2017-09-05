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

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

public class NomsServer
        implements AutoCloseable
{
    private static final String NOMS_BINARY;

    static {
        String goPath = System.getenv("GOPATH");
        if (goPath == null) {
            goPath = System.getProperty("user.home") + "/go";
        }
        NOMS_BINARY = goPath + "/bin/noms";
    }

    private Process process;
    private URI uri;

    public static NomsServer start(String dbPath)
    {
        return new NomsServer(dbPath);
    }

    public NomsServer(String dbPath)
    {
        int port = findFreePort();
        ProcessBuilder b = new ProcessBuilder(NOMS_BINARY, "serve", "--port=" + port, dbPath);
        b.inheritIO();
        try {
            process = b.start();
            uri = URI.create("http://localhost:" + port);
            if (!process.isAlive()) {
                throw new RuntimeException("Failed to start: " + String.join(" ", b.command()) + ": " + process.exitValue());
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public URI uri()
    {
        return uri;
    }

    public String exec(String command)
    {
        ProcessBuilder b = new ProcessBuilder("noms", command, uri.toString());
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

    public void stop()
    {
        process.destroyForcibly();
    }

    private int findFreePort()
    {
        try (ServerSocket socket = new ServerSocket(0);) {
            return socket.getLocalPort();
        }
        catch (IOException e) {
            throw new RuntimeException("unexpected", e);
        }
    }

    public void close()
    {
        stop();
    }
}
