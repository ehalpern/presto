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

import io.airlift.log.Logger;

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.URI;

public class NomsServer
        implements AutoCloseable
{
    private static final Logger log = Logger.get(NomsServer.class);

    private Process process;
    private URI uri;
    private String dbPath;

    public static NomsServer start(String dbPath)
    {
        return (new NomsServer(dbPath)).waitForStart();
    }

    public NomsServer(String dbPath)
    {
        int port = findFreePort();
        ProcessBuilder b = new ProcessBuilder(NomsRunner.NOMS_BINARY, "serve", "--port=" + port, dbPath);
        b.environment().put("NOMS_VERSION_NEXT", "1");
        b.inheritIO();
        this.dbPath = dbPath;
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

    private NomsServer waitForStart()
    {
        long timeout = 200;
        log.info("Starting " + this);
        for (int retry = 0; true; retry++) {
            try {
                uri.toURL().getContent();
                log.info("Running " + this);
                return this;
            }
            catch (ConnectException e) {
                if (retry > 4) {
                    break;
                }
                try {
                    timeout *= (retry + 1);
                    Thread.sleep(timeout);
                }
                catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        log.warn("Timed out waiting for %s after %d ms. Continuing anyway", this, timeout);
        return this;
    }

    public void stop()
    {
        log.info("Stopping " + this);
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

    public String toString()
    {
        return String.format("[noms serve %s](%s)", dbPath, uri);
    }
}
