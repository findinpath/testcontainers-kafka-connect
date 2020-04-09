package com.findinpath.testcontainers;

import org.slf4j.Logger;
import org.testcontainers.containers.output.OutputFrame;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.function.Consumer;

final class Utils {
    public static final String CONFLUENT_PLATFORM_VERSION = "5.3.1";

    private Utils() {
    }

    /**
     * Retrieves a random port that is currently not in use on this machine.
     *
     * @return a free port
     * @throws IOException wraps the exceptions which may occur during this method call.
     */
    static int getRandomFreePort() throws IOException {
        try (var serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        }
    }

    /**
     * Add logs consumer
     *
     * @param log
     * @return
     */
    public static Consumer<OutputFrame> containerLogsConsumer(Logger log) {
        return (OutputFrame outputFrame) -> log.trace(outputFrame.getUtf8String());
    }
}
