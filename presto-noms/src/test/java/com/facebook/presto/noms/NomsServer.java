package com.facebook.presto.noms;

import org.codehaus.plexus.util.IOUtil;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

public class NomsServer {
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

    public NomsServer(String dbPath) {
        ProcessBuilder b = new ProcessBuilder(NOMS_BINARY, "serve", "--port=18000", dbPath);
        b.inheritIO();
        try {
            process = b.start();
            uri = URI.create("http://localhost:18000");
            if (!process.isAlive()) {
                throw new RuntimeException("Failed to start: " + String.join(" ", b.command()) + ": " + process.exitValue());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public URI uri() {
        return uri;
    }

    public String exec(String command) {
        ProcessBuilder b = new ProcessBuilder("noms", command, uri.toString());
        try {
            Process p = b.start();
            String result = IOUtil.toString(p.getInputStream());
            if (!p.waitFor(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("timed out waiting for command to complete");
            }
            if (p.exitValue() != 0) {
                throw new RuntimeException("command failed: " + p.exitValue() + " :" + IOUtil.toString(p.getErrorStream()));
            }
            return result;
        } catch (IOException|InterruptedException e) {
          throw new RuntimeException(e);
        }
    }

    public void stop() {
        process.destroyForcibly();
    }
}
