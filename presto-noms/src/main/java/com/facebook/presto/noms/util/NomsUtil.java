package com.facebook.presto.noms.util;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class NomsUtil {
    private static final String NOMS_BINARY;

    static {
        String goPath = System.getenv("GOPATH");
        if (goPath == null) {
            goPath = System.getProperty("user.home") + "/go";
        }
        NOMS_BINARY = goPath + "/bin/noms";
    }

    private static String exec(String... command) {
        ProcessBuilder b = new ProcessBuilder(command);
        try {
            Process p = b.start();
            String result = IOUtils.toString(p.getInputStream());
            if (!p.waitFor(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("timed out waiting for command to complete");
            }
            if (p.exitValue() != 0) {
                throw new RuntimeException("command failed: " + p.exitValue() + " :" + IOUtils.toString(p.getErrorStream()));
            }
            return result;
        } catch (IOException|InterruptedException e) {
          throw new RuntimeException(e);
        }
    }

    public static List<String> ds(String dbPath) {
        String result = exec(NOMS_BINARY, "ds", dbPath);
        return ImmutableList.copyOf(result.split("\n"));
    }
}
