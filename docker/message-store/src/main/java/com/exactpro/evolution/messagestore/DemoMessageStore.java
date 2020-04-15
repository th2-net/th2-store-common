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
import com.exactpro.evolution.api.phase_1.ConnectivityGrpc;
import com.exactpro.evolution.api.phase_1.ConnectivityGrpc.ConnectivityBlockingStub;
import com.exactpro.evolution.api.phase_1.Message;
import com.exactpro.evolution.api.phase_1.QueueInfo;
import com.exactpro.evolution.api.phase_1.QueueRequest;
import com.exactpro.evolution.api.phase_1.SessionId;
import com.exactpro.evolution.common.CassandraConfig;
import com.exactpro.evolution.common.Configuration;
import com.exactpro.evolution.common.TcpCradleStream;
import com.exactpro.evolution.configuration.RabbitMQConfiguration;
import com.exactpro.evolution.configuration.Th2Configuration.Address;
import com.rabbitmq.client.Delivery;
import org.apache.mina.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.exactpro.evolution.common.Configuration.readConfiguration;
import static io.grpc.ManagedChannelBuilder.forAddress;

public class DemoMessageStore {
    private final static Logger LOGGER = LoggerFactory.getLogger(DemoMessageStore.class);
    private final Configuration configuration;
    private final List<Subscriber> subscribers;
    private CradleManager cradleManager;
    private final Set<String> streamNames = new ConcurrentHashSet<>();

    public DemoMessageStore(Configuration configuration) {
        this.configuration = configuration;
        this.subscribers = createSubscribers(configuration.getRabbitMQ(), configuration.getTh2().getConnectivityAddresses());
    }

    public void init() throws CradleStorageException {
        CassandraConfig cassandraConfig = configuration.getCassandraConfig();
        cradleManager = new CassandraCradleManager(new CassandraConnection(cassandraConfig.getConnectionSettings()));
        cradleManager.init(configuration.getCradleInstanceName());
    }

    public void startAndBlock() throws InterruptedException {
        subscribers.forEach(Subscriber::start);
        synchronized (this) {
            wait();
        }
    }

    public void dispose() {
        subscribers.forEach(Subscriber::dispose);
        try {
            cradleManager.dispose();
        } catch (CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private List<Subscriber> createSubscribers(RabbitMQConfiguration rabbitMQ,
                                               Map<String, Address> connectivityServices) {
        List<Subscriber> subscribers = new ArrayList<>();
        for (Address address : connectivityServices.values()) {
            subscribers.add(createSubscriber(rabbitMQ, address));
        }
        return Collections.unmodifiableList(subscribers);
    }

    private Subscriber createSubscriber(RabbitMQConfiguration rabbitMQ, Address address) {
        ConnectivityBlockingStub connectivityBlockingStub = ConnectivityGrpc.newBlockingStub(
                forAddress(address.getHost(), address.getPort()).usePlaintext().build());
        QueueInfo queueInfo = connectivityBlockingStub.getQueueInfo(QueueRequest.newBuilder().build());
        RabbitMqSubscriber outMsgSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
                this::processOutMessage, null, queueInfo.getOutMsgQueue());
        RabbitMqSubscriber inMsgSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
                this::processInMessage, null, queueInfo.getInMsgQueue());
        return new Subscriber(rabbitMQ, inMsgSubscriber, outMsgSubscriber);
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
            LOGGER.debug("Message stored: {}", storedMessage);
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

    private static class Subscriber {
        private final RabbitMQConfiguration rabbitMQ;
        private final RabbitMqSubscriber inSubscriber;
        private final RabbitMqSubscriber outSubscriber;

        private Subscriber(RabbitMQConfiguration rabbitMQ, RabbitMqSubscriber inSubscriber, RabbitMqSubscriber outSubscriber) {
            this.rabbitMQ = rabbitMQ;
            this.inSubscriber = inSubscriber;
            this.outSubscriber = outSubscriber;
        }

        private void start() {
            subscribe(inSubscriber);
            subscribe(outSubscriber);
        }

        private void dispose() {
            dispose(inSubscriber);
            dispose(outSubscriber);
        }

        private void dispose(RabbitMqSubscriber subscriber) {
            try {
                subscriber.close();
            } catch (Exception e) {
                LOGGER.error("Could not dispose the mq subscriber", e);
            }
        }

        private void subscribe(RabbitMqSubscriber subscriber) {
            try {
                subscriber.startListening(rabbitMQ.getHost(), rabbitMQ.getVirtualHost(), rabbitMQ.getPort(),
                        rabbitMQ.getUsername(), rabbitMQ.getPassword());
            } catch (Exception e) {
                LOGGER.error("Could not subscribe to queue", e);
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration configuration = readConfiguration(args);
            DemoMessageStore messageStore = new DemoMessageStore(configuration);
            messageStore.init();
            messageStore.startAndBlock();
            Runtime.getRuntime().addShutdownHook(new Thread(messageStore::dispose));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
