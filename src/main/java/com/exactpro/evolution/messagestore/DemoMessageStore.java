/******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
 *
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
 ******************************************************************************/
package com.exactpro.evolution.messagestore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.StoredMessage;
import com.exactpro.cradle.StoredMessageId;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.evolution.RabbitMqSubscriber;
import com.exactpro.evolution.api.phase_1.Message;
import com.exactpro.evolution.api.phase_1.SessionId;
import com.exactpro.evolution.common.CassandraConfig;
import com.exactpro.evolution.common.TcpCradleStream;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import org.apache.mina.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static java.lang.System.getenv;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.math.NumberUtils.toInt;

public class DemoMessageStore {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + "@" + hashCode());

    private static final String ENV_RABBIT_QM_HOST = "RABBIT_QM_HOST";
    private static final String ENV_RABBIT_QM_PORT = "RABBIT_QM_PORT";
    private static final String ENV_RABBIT_QM_USER = "RABBIT_QM_USER";
    private static final String ENV_RABBIT_QM_PASS = "RABBIT_QM_PASS";
    private static final String ENV_RABBIT_QM_VHOST = "RABBIT_QM_VHOST";

    private RabbitMqSubscriber inMsgSubscriber;
    private RabbitMqSubscriber outMsgSubscriber;
    private CradleManager cradleManager;
    private final Set<String> streamNames = new ConcurrentHashSet<>();

    public void init() throws IOException, CradleStorageException, TimeoutException {
        CassandraConfig cassandraConfig = CassandraConfig.loadFrom(new File("EventStore.cfg"));
        cradleManager = new CassandraCradleManager(new CassandraConnection(cassandraConfig.getConnectionSettings()));
        cradleManager.init(cassandraConfig.getInstanceName());
        // FIXME get info about connectivities from variables or config and request QueueInfo
        outMsgSubscriber = new RabbitMqSubscriber("demo_exchange",
            this::processOutMessage,
            null,
            "fix_client_out");
        inMsgSubscriber = new RabbitMqSubscriber("demo_exchange",
            this::processInMessage,
            null,
            "fix_client_in");
        String host = defaultIfNull(getenv(ENV_RABBIT_QM_HOST), ConnectionFactory.DEFAULT_HOST);
        String vHost = defaultIfNull(getenv(ENV_RABBIT_QM_VHOST), ConnectionFactory.DEFAULT_VHOST);
        int port = defaultIfNull(toInt(getenv(ENV_RABBIT_QM_PORT)), ConnectionFactory.DEFAULT_AMQP_PORT);
        String username = defaultIfNull(getenv(ENV_RABBIT_QM_USER), ConnectionFactory.DEFAULT_USER);
        String password = defaultIfNull(getenv(ENV_RABBIT_QM_PASS), ConnectionFactory.DEFAULT_PASS);
        outMsgSubscriber.startListening(host, vHost, port, username, password);
        inMsgSubscriber.startListening(host, vHost, port, username, password);
    }

    public void start() throws InterruptedException {
        synchronized (this) {
            wait();
        }
    }

    public void dispose() {
        try {
            if (inMsgSubscriber != null) {
                inMsgSubscriber.close();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        try {
            if (outMsgSubscriber != null) {
                outMsgSubscriber.close();
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        try {
            cradleManager.dispose();
        } catch (CradleStorageException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void processInMessage(String consumerTag, Delivery delivery) {
        storeMessage(delivery, Direction.RECEIVED);
    }

    private void processOutMessage(String consumerTag, Delivery delivery) {
        storeMessage(delivery, Direction.SENT);
    }

    private void storeMessage(Delivery delivery, Direction direction) {
        try {
            Message message = Message.parseFrom(delivery.getBody());
            String streamName = storeStreamIfNeeded(message);
            StoredMessage storedMessage = toStoredMessage(delivery.getBody(), message, streamName, direction);
            cradleManager.getStorage().storeMessage(storedMessage);
            System.out.println("Message stored: " + storedMessage);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private StoredMessage toStoredMessage(byte[] body, Message message, String streamName, Direction direction) {
        StoredMessage storedMessage = new StoredMessage();
        storedMessage.setContent(body);
        storedMessage.setDirection(direction);
        StoredMessageId storedMessageId = new StoredMessageId();
        storedMessageId.setId(message.getMetadata().getMessageId());
        storedMessage.setId(storedMessageId);
        storedMessage.setStreamName(streamName);
        storedMessage.setTimestamp(Instant.now());
        return storedMessage;
    }

    private String storeStreamIfNeeded(Message message) throws IOException {
        SessionId sessionId = message.getMetadata().getConnectivityId().getSessionId();
        String sessionAlias = sessionId.getSessionAlias();
        if (streamNames.add(sessionAlias)) {
            TcpCradleStream tcpCradleStream = new TcpCradleStream(
                sessionId.getSessionAlias(),
                sessionId.getSourceAddress().getHost() + ":" + sessionId.getSourceAddress().getPort(),
                sessionId.getTargetAddress().getHost() + ":" + sessionId.getTargetAddress().getPort()
            );
            cradleManager.getStorage().storeStream(tcpCradleStream);
        }
        return sessionAlias;
    }

    public static void main(String[] args) throws TimeoutException, IOException, CradleStorageException, InterruptedException {
        DemoMessageStore messageStore = new DemoMessageStore();
        messageStore.init();
        messageStore.start();
        Runtime.getRuntime().addShutdownHook(new Thread(messageStore::dispose));
    }
}
