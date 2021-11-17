/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
 */

package com.exactpro.th2.store.common.utils;

import java.time.Instant;
import java.util.UUID;
import java.util.stream.Collectors;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventBatchToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStore;
import com.exactpro.cradle.testevents.TestEventSingleToStoreBuilder;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.Event;
import com.exactpro.th2.common.grpc.EventBatchOrBuilder;
import com.exactpro.th2.common.grpc.EventIDOrBuilder;
import com.exactpro.th2.common.grpc.EventOrBuilder;
import com.exactpro.th2.common.grpc.EventStatus;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageIDOrBuilder;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;

public class ProtoUtil {
    public static StoredMessageId toStoredMessageId(MessageIDOrBuilder messageId, TimestampOrBuilder timestamp) {
        return new StoredMessageId(
                new BookId(messageId.getBookName()),
                messageId.getConnectionId().getSessionAlias(),
                toCradleDirection(messageId.getDirection()),
                toInstant(timestamp),
                messageId.getSequence()
        );
    }

    @Deprecated
    public static TestEventBatchToStore toCradleBatch(
            EventBatchOrBuilder protoEventBatch,
            String scope,
            String parentScope,
            Timestamp startTimestamp
    ) throws CradleStorageException {
        TestEventBatchToStore cradleBatch = TestEventToStore
            .batchBuilder(CradleStorage.DEFAULT_MAX_TEST_EVENT_BATCH_SIZE)
            .id(new BookId(protoEventBatch.getParentEventId().getBookName()), scope, toInstant(startTimestamp), UUID.randomUUID().toString())
            .parentId(toCradleEventID(protoEventBatch.getParentEventId(), scope, startTimestamp))
            .build();
        for (Event protoEvent : protoEventBatch.getEventsList()) {
            cradleBatch.addTestEvent(toCradleEvent(protoEvent, scope, parentScope, startTimestamp));
        }
        return cradleBatch;
    }

    public static StoredTestEventId toCradleEventID(EventIDOrBuilder protoEventID, String scope, TimestampOrBuilder startTimestamp) {
        return new StoredTestEventId(
                new BookId(protoEventID.getBookName()),
                scope,
                toInstant(startTimestamp),
                String.valueOf(protoEventID.getId())
        );
    }

    /**
     * Converts protobuf event status to success Gradle status
     */
    public static boolean isSuccess(EventStatus protoEventStatus) {
        switch (protoEventStatus) {
        case SUCCESS:
            return true;
        case FAILED:
            return false;
        default:
            throw new IllegalArgumentException("Unknown the event status '" + protoEventStatus + '\'');
        }
    }

    public static TestEventSingleToStore toCradleEvent(
            EventOrBuilder protoEvent,
            String scope,
            String parentScope,
            Timestamp parentTimestamp
    ) throws CradleStorageException {
        TestEventSingleToStoreBuilder cradleEventBuilder = TestEventToStore
            .singleBuilder()
            .id(toCradleEventID(protoEvent.getId(), scope, protoEvent.getStartTimestamp()))
            .name(protoEvent.getName())
            .type(protoEvent.getType())
            .success(isSuccess(protoEvent.getStatus()))
            .messages(protoEvent.getAttachedMessageIdsList().stream()
                    .map(messageId -> toStoredMessageId(messageId, protoEvent.getStartTimestamp()))
                    .collect(Collectors.toSet()))
            .content(protoEvent.getBody().toByteArray());

        if (protoEvent.hasParentId()) {
            cradleEventBuilder.parentId(toCradleEventID(protoEvent.getParentId(), parentScope, parentTimestamp));
        }
        if (protoEvent.hasEndTimestamp()) {
            cradleEventBuilder.endTimestamp(toInstant(protoEvent.getEndTimestamp()));
        }
        return cradleEventBuilder.build();
    }

    public static MessageToStore toCradleMessage(Message protoMessage) throws CradleStorageException {
        MessageMetadata metadata = protoMessage.getMetadata();
        MessageID messageID = metadata.getId();
        return new MessageToStoreBuilder()
            .bookId(new BookId(protoMessage.getMetadata().getId().getBookName()))
            .sessionAlias(messageID.getConnectionId().getSessionAlias())
            .direction(toCradleDirection(messageID.getDirection()))
            .timestamp(toInstant(metadata.getTimestamp()))
            .sequence(messageID.getSequence())
            .content(protoMessage.toByteArray())
            .build();
    }

    public static MessageToStore toCradleMessage(RawMessage protoRawMessage) throws CradleStorageException {
        RawMessageMetadata metadata = protoRawMessage.getMetadata();
        MessageID messageID = metadata.getId();
        return new MessageToStoreBuilder()
            .bookId(new BookId(protoRawMessage.getMetadata().getId().getBookName()))
            .sessionAlias(messageID.getConnectionId().getSessionAlias())
            .direction(toCradleDirection(messageID.getDirection()))
            .timestamp(toInstant(metadata.getTimestamp()))
            .sequence(messageID.getSequence())
            .content(protoRawMessage.toByteArray())
            .build();
    }

    public static Direction toCradleDirection(com.exactpro.th2.common.grpc.Direction protoDirection) {
        switch (protoDirection) {
        case FIRST:
            return Direction.FIRST;
        case SECOND:
            return Direction.SECOND;
        default:
            throw new IllegalArgumentException("Unknown the direction type '" + protoDirection + '\'');
        }
    }

    public static com.exactpro.th2.common.grpc.Direction toProtoDirection(Direction cradleDirection) {
        switch (cradleDirection) {
        case FIRST:
            return com.exactpro.th2.common.grpc.Direction.FIRST;
        case SECOND:
            return com.exactpro.th2.common.grpc.Direction.SECOND;
        default:
            throw new IllegalArgumentException("Unknown the direction type '" + cradleDirection + '\'');
        }
    }

    public static Instant toInstant(TimestampOrBuilder timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
