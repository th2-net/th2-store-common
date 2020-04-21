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
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.evolution.RabbitMqSubscriber;
import com.exactpro.evolution.api.phase_1.Batch;
import com.exactpro.evolution.api.phase_1.ConnectivityGrpc.ConnectivityBlockingStub;
import com.exactpro.evolution.api.phase_1.Message;
import com.exactpro.evolution.api.phase_1.MessageId;
import com.exactpro.evolution.api.phase_1.QueueInfo;
import com.exactpro.evolution.api.phase_1.QueueRequest;
import com.exactpro.evolution.api.phase_1.RawMessage;
import com.exactpro.evolution.common.CassandraConfig;
import com.exactpro.evolution.common.Configuration;
import com.exactpro.evolution.configuration.RabbitMQConfiguration;
import com.exactpro.evolution.configuration.Th2Configuration.Address;
import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.Delivery;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.exactpro.cradle.messages.StoredMessageBatch.singleton;
import static com.exactpro.evolution.api.phase_1.ConnectivityGrpc.newBlockingStub;
import static com.exactpro.evolution.common.Configuration.readConfiguration;
import static com.exactpro.evolution.common.utils.ProtoUtil.toBatchId;
import static com.exactpro.evolution.common.utils.ProtoUtil.toStoredMessageId;
import static io.grpc.ManagedChannelBuilder.forAddress;

public class DemoMessageStore {
    private final static Logger LOGGER = LoggerFactory.getLogger(DemoMessageStore.class);
    private final Configuration configuration;
    private final List<Subscriber> subscribers;
    private CradleManager cradleManager;

    public DemoMessageStore(Configuration configuration) {
        this.configuration = configuration;
        this.subscribers = createSubscribers(configuration.getRabbitMQ(), configuration.getTh2().getConnectivityAddresses());
    }

    public void init() throws CradleStorageException {
        CassandraConfig cassandraConfig = configuration.getCassandraConfig();
        cradleManager = new CassandraCradleManager(new CassandraConnection(cassandraConfig.getConnectionSettings()));
        cradleManager.init(configuration.getCradleInstanceName());
        LOGGER.info("cradle init successfully with {} instance name", configuration.getCradleInstanceName() );
    }

    public void startAndBlock() throws InterruptedException {
        subscribers.forEach(Subscriber::start);
        LOGGER.info("message store started");
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
            Subscriber subscriber = createSubscriber(rabbitMQ, address);
            if (subscriber != null) {
                subscribers.add(subscriber);
            }
        }
        return Collections.unmodifiableList(subscribers);
    }

    private Subscriber createSubscriber(RabbitMQConfiguration rabbitMQ, Address address) {
        ManagedChannel channel = forAddress(address.getHost(), address.getPort()).usePlaintext().build();
        try {
            ConnectivityBlockingStub connectivityBlockingStub = newBlockingStub(channel);
            QueueInfo queueInfo = connectivityBlockingStub.getQueueInfo(QueueRequest.newBuilder().build());
            LOGGER.info("get QueueInfo for {}:{} {}", address.getHost(), address.getPort(), queueInfo);
            RabbitMqSubscriber outMsgSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
                    (consumerTag, delivery) -> storeMessage(delivery, Direction.RECEIVED),
                    null, queueInfo.getOutMsgQueue());
            RabbitMqSubscriber inMsgSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
                    (consumerTag, delivery) -> storeMessage(delivery, Direction.SENT),
                    null, queueInfo.getInMsgQueue());
            RabbitMqSubscriber outRawMsgSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
                    (consumerTag, delivery) -> storeMessageBatch(delivery, Direction.RECEIVED),
                    null, queueInfo.getOutRawMsgQueue());
            RabbitMqSubscriber inRawMsgSubscriber = new RabbitMqSubscriber(queueInfo.getExchangeName(),
                    (consumerTag, delivery) -> storeMessageBatch(delivery, Direction.SENT),
                    null, queueInfo.getInRawMsgQueue());
            return new Subscriber(rabbitMQ, inMsgSubscriber, outMsgSubscriber, inRawMsgSubscriber, outRawMsgSubscriber);
        } catch (Exception e) {
            LOGGER.error("Could not create subscriber for {}:{}", address.getHost(), address.getPort(), e);
        }
        finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
        return null;
    }

    private void storeMessage(Delivery delivery, Direction direction) {
        try {
            Message message = Message.parseFrom(delivery.getBody());
            StoredMessageBatch storedMessageBatch = toStoredMessageBatch(delivery.getBody(),
                    message.getMetadata().getConnectivityId().getConnectivityId(),
                    message.getMetadata().getMessageId(), direction);
            cradleManager.getStorage().storeMessageBatch(storedMessageBatch);
            LOGGER.debug("Singleton Batch Message stored: {}", storedMessageBatch.getId().getId());
        } catch (Exception e) {
            LOGGER.error("Could not store message", e);
        }
    }

    private void storeMessageBatch(Delivery delivery, Direction direction) {
        try {
            StoredMessageBatch storedMessageBatch = toStoreMessageBatch(delivery.getBody(), direction);
            cradleManager.getStorage().storeMessageBatch(storedMessageBatch);
            LOGGER.debug("Batch Message stored: {}:{}",
                    storedMessageBatch.getId().getId(), storedMessageBatch.getMessageCount());
        } catch (Exception e) {
            LOGGER.error("Could not store message", e);
        }
    }

    private StoredMessageBatch toStoreMessageBatch(byte[] protoBatchData, Direction direction)
            throws CradleStorageException, InvalidProtocolBufferException {
        Batch protoBatch = Batch.parseFrom(protoBatchData);
        LOGGER.debug("Proto batch received: {}", protoBatch);
        StoredMessageBatch storedMessageBatch = new StoredMessageBatch(toBatchId(protoBatch.getBatchId()));
        for (RawMessage rawMessage : protoBatch.getMessageList()) {
            storedMessageBatch.addMessage(createStoredMessage(rawMessage.getData().toByteArray(), direction,
                    toStoredMessageId(rawMessage.getMessageId()), protoBatch.getConnectivityId().getConnectivityId()));
        }
        return storedMessageBatch;
    }

    private StoredMessage createStoredMessage(byte[] body, Direction direction, StoredMessageId messageId, String streamName) {
        StoredMessage storedMessage = new StoredMessage();
        storedMessage.setContent(body);
        storedMessage.setDirection(direction);
        storedMessage.setId(messageId);
        storedMessage.setStreamName(streamName);
        storedMessage.setTimestamp(Instant.now());
        return storedMessage;
    }

    private StoredMessageBatch toStoredMessageBatch(byte[] body, String streamName, MessageId messageId, Direction direction)
            throws CradleStorageException {
        return singleton(createStoredMessage(body, direction, toStoredMessageId(messageId), streamName));
    }

    private static class Subscriber {
        private final RabbitMQConfiguration rabbitMQ;
        private final RabbitMqSubscriber inSubscriber;
        private final RabbitMqSubscriber outSubscriber;
        private final RabbitMqSubscriber inRawSubscriber;
        private final RabbitMqSubscriber outRawSubscriber;

        private Subscriber(RabbitMQConfiguration rabbitMQ, RabbitMqSubscriber inSubscriber,
                           RabbitMqSubscriber outSubscriber, RabbitMqSubscriber inRawMsgSubscriber,
                           RabbitMqSubscriber outRawMsgSubscriber) {
            this.rabbitMQ = rabbitMQ;
            this.inSubscriber = inSubscriber;
            this.outSubscriber = outSubscriber;
            this.inRawSubscriber = inRawMsgSubscriber;
            this.outRawSubscriber = outRawMsgSubscriber;
        }

        private void start() {
            subscribe(inSubscriber);
            subscribe(outSubscriber);
            subscribe(inRawSubscriber);
            subscribe(outRawSubscriber);
        }

        private void dispose() {
            dispose(inSubscriber);
            dispose(outSubscriber);
            dispose(inRawSubscriber);
            dispose(outRawSubscriber);
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
            LOGGER.error(e.getMessage(), e);
            LOGGER.error("Error occurred. Exit the program");
            System.exit(-1);
        }
    }
}
