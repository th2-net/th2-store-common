/*******************************************************************************
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.exactpro.th2.store.common;

import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQHost;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQPass;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQPort;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQUser;
import static com.exactpro.th2.configuration.RabbitMQConfiguration.getEnvRabbitMQVhost;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class CustomRabbitSubscriber implements Closeable {

    private static final int CLOSE_TIMEOUT = 1_000;
    private static final Logger logger = LoggerFactory.getLogger(CustomRabbitSubscriber.class);
    private final String exchangeName;
    private final String[] queues;
    private final DeliverCallback deliverCallback;
    private final CancelCallback cancelCallback;
    private Connection connection;
    private Channel channel;

    public CustomRabbitSubscriber(String exchangeName,
            DeliverCallback deliverCallback,
            CancelCallback cancelCallback,
            String... queues) {
        this.exchangeName = Objects.requireNonNull(exchangeName, "exchange name is null");
        this.deliverCallback = deliverCallback;
        this.cancelCallback = cancelCallback;
        this.queues = Objects.requireNonNull(queues, "queueNames is null");
    }

    public void startListening() throws IOException, TimeoutException {
        startListening(getEnvRabbitMQHost(), getEnvRabbitMQVhost(), getEnvRabbitMQPort(), getEnvRabbitMQUser(), getEnvRabbitMQPass(), null);
    }

    public void startListening(String host, String vHost, int port, String username, String password) throws IOException, TimeoutException {
        this.startListening(host, vHost, port, username, password, null);
    }

    public void startListening(String host, String vHost, int port, String username, String password, @Nullable String subscriberName) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        if (StringUtils.isNotEmpty(vHost)) {
            factory.setVirtualHost(vHost);
        }
        factory.setPort(port);
        if (StringUtils.isNotEmpty(username)) {
            factory.setUsername(username);
        }
        if (StringUtils.isNotEmpty(password)) {
            factory.setPassword(password);
        }
        connection = factory.newConnection();
        channel = connection.createChannel();
        subscribeToRoutes(exchangeName, subscriberName, queues);
    }

    @Override
    public void close() throws IOException {
        if (connection != null && connection.isOpen()) {
            connection.close(CLOSE_TIMEOUT);
        }
    }

    private void subscribeToRoutes(String exchangeName, String subscriberName, String[] queues)
            throws IOException {
        for (String queue : queues) {
            channel.basicConsume(queue, true, deliverCallback, consumerTag -> logger.info("consuming cancelled for {}", consumerTag));
            logger.info("Start listening exchangeName='{}', queue='{}'", exchangeName, queue);
        }
    }
}
