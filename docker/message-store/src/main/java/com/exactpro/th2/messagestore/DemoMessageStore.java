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
package com.exactpro.th2.messagestore;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageBatchId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.RabbitMqSubscriber;
import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.configuration.Th2Configuration.Address;
import com.exactpro.th2.connectivity.grpc.ConnectivityGrpc.ConnectivityBlockingStub;
import com.exactpro.th2.connectivity.grpc.QueueInfo;
import com.exactpro.th2.connectivity.grpc.QueueRequest;
import com.exactpro.th2.infra.grpc.*;
import com.exactpro.th2.store.common.CassandraConfig;
import com.exactpro.th2.store.common.Configuration;
import com.google.protobuf.MessageLite;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import io.grpc.ManagedChannel;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.exactpro.cradle.messages.StoredMessageBatch.MAX_MESSAGES_NUMBER;
import static com.exactpro.th2.connectivity.grpc.ConnectivityGrpc.newBlockingStub;
import static com.exactpro.th2.store.common.Configuration.readConfiguration;
import static io.grpc.ManagedChannelBuilder.forAddress;
import static javax.xml.bind.DatatypeConverter.printHexBinary;

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
            RabbitMqSubscriber outMsgSubscriber = createRabbitMqSubscriber(queueInfo.getOutMsgQueue(),
                    queueInfo.getExchangeName(),
                    (consumerTag, delivery) -> storeMessageBatch(delivery, Direction.RECEIVED));
            RabbitMqSubscriber inMsgSubscriber = createRabbitMqSubscriber(queueInfo.getInMsgQueue(),
                    queueInfo.getExchangeName(),
                    (consumerTag, delivery) -> storeMessageBatch(delivery, Direction.SENT));
            RabbitMqSubscriber outRawMsgSubscriber = createRabbitMqSubscriber(queueInfo.getOutRawMsgQueue(),
                    queueInfo.getExchangeName(),
                    (consumerTag, delivery) -> storeRawMessageBatch(delivery, Direction.RECEIVED));
            RabbitMqSubscriber inRawMsgSubscriber = createRabbitMqSubscriber(queueInfo.getInRawMsgQueue(),
                    queueInfo.getExchangeName(),
                    (consumerTag, delivery) -> storeRawMessageBatch(delivery, Direction.SENT));
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

    private void storeMessageBatch(Delivery delivery, Direction direction) {
        try {
            MessageBatch batch = MessageBatch.parseFrom(delivery.getBody());
            List<Message> messagesList = batch.getMessagesList();
            storeMessages(messagesList, direction, message -> message.getMetadata().getId());
        } catch (Exception e) {
            LOGGER.error("'{}':'{}' could not store message.",
                    delivery.getEnvelope().getExchange(), delivery.getEnvelope().getRoutingKey(), e);
            LOGGER.error("message body: {}", printHexBinary(delivery.getBody()));
        }
    }

    private void storeRawMessageBatch(Delivery delivery, Direction direction) {
        try {
            RawMessageBatch batch = RawMessageBatch.parseFrom(delivery.getBody());
            List<RawMessage> messagesList = batch.getMessagesList();
            storeMessages(messagesList, direction, rawMessage -> rawMessage.getMetadata().getId());
        } catch (Exception e) {
            LOGGER.error("'{}':'{}' could not store message batch.",
                    delivery.getEnvelope().getExchange(), delivery.getEnvelope().getRoutingKey(), e);
            LOGGER.error("batch body: {}", printHexBinary(delivery.getBody()));
        }
    }

    private <T extends MessageLite> void storeMessages(List<T> messagesList, Direction direction, Function<T, MessageID> extractIdFunction) throws CradleStorageException, IOException {
        if (messagesList.isEmpty()) {
            LOGGER.warn("Empty batch has been received"); //TODO: need identify
            return;
        }

        LOGGER.debug("Process {} messages started", messagesList.size());
        for(int from = 0; from < messagesList.size(); from += MAX_MESSAGES_NUMBER) {
            List<T> storedMessages = messagesList.subList(from, Math.min(from + MAX_MESSAGES_NUMBER, messagesList.size()));

            MessageID firstMessageId = extractIdFunction.apply(storedMessages.get(0));
            String streamName = firstMessageId.getConnectionId().getSessionAlias();
            StoredMessageBatchId storedBatchId = new StoredMessageBatchId(String.valueOf(firstMessageId.getSequence()));
            StoredMessageBatch storedMessageBatch = new StoredMessageBatch(storedBatchId);

            for (int index = 0; index < storedMessages.size(); index++) {
                StoredMessageId storedMessageId = new StoredMessageId(storedBatchId, index);
                T message = storedMessages.get(index);

                storedMessageBatch.addMessage(createStoredMessage(message.toByteArray(),
                        direction, //FIXME: Determine direction with data from message
                        storedMessageId,
                        streamName));
            }
            cradleManager.getStorage().storeMessageBatch(storedMessageBatch);
            LOGGER.debug("Message Batch stored: stream '{}', id '{}', size '{}'", streamName, storedMessageBatch.getId().getId(), storedMessageBatch.getStoredMessagesCount());
        }
        LOGGER.debug("Process {} messages finished", messagesList.size());
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

    private RabbitMqSubscriber createRabbitMqSubscriber(String queueName, String exchangeName, DeliverCallback deliverCallback) {
        return StringUtils.isEmpty(queueName) ? null : new RabbitMqSubscriber(exchangeName, deliverCallback, null, queueName);
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
            if (subscriber == null) {
                return;
            }
            try {
                subscriber.close();
            } catch (Exception e) {
                LOGGER.error("Could not dispose the mq subscriber", e);
            }
        }

        private void subscribe(RabbitMqSubscriber subscriber) {
            if (subscriber == null) {
                return;
            }
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
