package com.jene.nlp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Load configuration from a file
 *
 * @author Jorge Nieves (jene)
 */
public class Configs {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataAssembler.class);

    private static final String DEFAULT_CONFIG_File = "local.config.properties";

    static Properties loadConfig() throws IOException {
        return loadConfig(DEFAULT_CONFIG_File);
    }

    static Properties loadConfig(String configFile) throws IOException {

        LOGGER.info("Loading configs from:" + configFile);
        final Properties cfg = new Properties();

        try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile)) {
            cfg.load(inputStream);
        } catch (Exception ex) {
            LOGGER.error("Error loading configs from:" + configFile);
        }

        return cfg;
    }
}
