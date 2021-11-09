package io.aiven.connect.jdbc.metrics;

import com.sun.net.httpserver.HttpServer;
import io.aiven.connect.jdbc.sink.JdbcSinkConfig;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * create by pengchuan.chen on 2021/11/4
 */
public class JdbcMetrics {

    private static final Logger logger = LoggerFactory.getLogger(JdbcMetrics.class);

    private static JdbcMetrics jdbcMetrics = null;

    private PrometheusMeterRegistry prometheusMeterRegistry;
    private int httpPort = 8008;
    private Thread httpServerThread;
    private HttpServer httpServer;


    public JdbcMetrics(int httpPort) {
        this.httpPort = httpPort;
        if (prometheusMeterRegistry == null) {
            prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            startHttpServer(prometheusMeterRegistry);
        }
    }

    public JdbcMetrics(PrometheusMeterRegistry prometheusMeterRegistry, int httpPort) {
        this.httpPort = httpPort;
        this.prometheusMeterRegistry = prometheusMeterRegistry;
        this.startHttpServer(prometheusMeterRegistry);
    }

    public synchronized static JdbcMetrics buildOrGetInstance(JdbcSinkConfig config) {
        if (config.metricsEnabled) {
            int port = config.metricsHttpPort;
            if (jdbcMetrics == null) {
                logger.info("init JdbcMetrics registry export port:{} .", port);
                jdbcMetrics = new JdbcMetrics(port);
            } else if (jdbcMetrics.httpPort != port) {
                logger.info("refresh metrics export port {} -> {} .", jdbcMetrics.httpPort, port);
                //TODO Entering here means that the port configuration has been modified and refreshed.
                if (jdbcMetrics.httpServerThread != null && jdbcMetrics.httpServer != null) {
                    jdbcMetrics.httpServer.stop(0);
                    jdbcMetrics.httpServerThread.interrupt();
                }
                //TODO To prevent restarting the httpserver port after refreshing, causing the loss of the metrics data in the front, so here prometheusMeterRegistry needs to be passed.
                jdbcMetrics = new JdbcMetrics(jdbcMetrics.prometheusMeterRegistry, port);
            }
        }
        return jdbcMetrics;
    }

    public void close() {
        if (prometheusMeterRegistry != null && !prometheusMeterRegistry.isClosed()) {
            logger.info("stopping metrics prometheusMeterRegistry...");
            prometheusMeterRegistry.close();
        }
        if (httpServer != null) {
            logger.info("stopping metrics httpServer({})...", httpPort);
            httpServer.stop(0);
        }
        if (!httpServerThread.isInterrupted()) {
            logger.info("interrupting metrics httpServerThread...");
            httpServerThread.interrupt();
        }
    }

    public PrometheusMeterRegistry getMeterRegistry() {
        return prometheusMeterRegistry;
    }

    private void startHttpServer(PrometheusMeterRegistry prometheusRegistry) {
        try {
            httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
            httpServer.createContext("/metrics", httpExchange -> {
                String response = prometheusRegistry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });

            httpServerThread = new Thread(httpServer::start);
            httpServerThread.setDaemon(true);
            httpServerThread.start();
        } catch (IOException e) {
            logger.warn("start metrics HttpServer failed.", e);
        }
    }
}