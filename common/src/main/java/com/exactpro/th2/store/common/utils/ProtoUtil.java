/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.MinimalTestEventToStore;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.TestEventToStore;
import com.exactpro.cradle.testevents.TestEventToStoreBuilder;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.eventstore.grpc.Event;
import com.exactpro.th2.eventstore.grpc.EventBatchOrBuilder;
import com.exactpro.th2.eventstore.grpc.EventID;
import com.exactpro.th2.eventstore.grpc.EventIDOrBuilder;
import com.exactpro.th2.eventstore.grpc.EventOrBuilder;
import com.exactpro.th2.eventstore.grpc.EventStatus;
import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.MessageID;
import com.exactpro.th2.infra.grpc.MessageIDOrBuilder;
import com.exactpro.th2.infra.grpc.MessageMetadata;
import com.exactpro.th2.infra.grpc.RawMessage;
import com.exactpro.th2.infra.grpc.RawMessageMetadata;
import com.google.protobuf.TimestampOrBuilder;

public class ProtoUtil {

    public static StoredMessageId toStoredMessageId(MessageIDOrBuilder messageId) {
        return new StoredMessageId(messageId.getConnectionId().getSessionAlias(), toCradleDirection(messageId.getDirection()), messageId.getSequence());
    }

    public static StoredTestEventBatch toCradleBatch(EventBatchOrBuilder protoEventBatch) throws CradleStorageException {
        StoredTestEventBatch cradleEventsBatch = StoredTestEvent.newStoredTestEventBatch(MinimalTestEventToStore.newMinimalTestEventBuilder()
            .id(new StoredTestEventId(Uuids.timeBased().toString()))
            .name("Batch of events") // TODO: Can this field have another value?
            .type("Multi-dummy") // TODO: Can this field have another value?
            .parentId(toCradleEventID(protoEventBatch.getParentEventId()))
            .build());
        for (Event protoEvent : protoEventBatch.getEventsList()) {
            cradleEventsBatch.addTestEvent(toCradleEvent(protoEvent));
        }
        return cradleEventsBatch;
    }

    public static StoredTestEventId toCradleEventID(EventIDOrBuilder protoEventID) {
        return new StoredTestEventId(String.valueOf(protoEventID.getId()));
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

    public static TestEventToStore toCradleEvent(EventOrBuilder protoEvent) {
        TestEventToStoreBuilder builder = TestEventToStore.newTestEventBuilder()
            .id(toCradleEventID(protoEvent.getId()))
            .name(protoEvent.getName())
            .type(protoEvent.getType())
            .success(isSuccess(protoEvent.getStatus()))
            .content(protoEvent.getBody().toByteArray());

        if (protoEvent.hasParentId()) {
            builder.parentId(toCradleEventID(protoEvent.getParentId()));
        }
        if (protoEvent.hasStartTimestamp()) {
            builder.startTimestamp(toInstant(protoEvent.getStartTimestamp()));
        }
        if (protoEvent.hasEndTimestamp()) {
            builder.endTimestamp(toInstant(protoEvent.getEndTimestamp()));
        }
        return builder.build();
    }

    public static MessageToStore toCradleMessage(Message protoMessage) {
        MessageMetadata metadata = protoMessage.getMetadata();
        MessageID messageID = metadata.getId();
        return new MessageToStoreBuilder()
            .streamName(messageID.getConnectionId().getSessionAlias())
            .content(protoMessage.toByteArray())
            .timestamp(toInstant(metadata.getTimestamp()))
            .direction(toCradleDirection(messageID.getDirection()))
            .index(messageID.getSequence())
            .build();
    }

    public static MessageToStore toCradleMessage(RawMessage protoRawMessage) {
        RawMessageMetadata metadata = protoRawMessage.getMetadata();
        MessageID messageID = metadata.getId();
        return new MessageToStoreBuilder()
            .streamName(messageID.getConnectionId().getSessionAlias())
            .content(protoRawMessage.toByteArray())
            .timestamp(toInstant(metadata.getTimestamp()))
            .direction(toCradleDirection(messageID.getDirection()))
            .index(messageID.getSequence())
            .build();
    }

    public static Direction toCradleDirection(com.exactpro.th2.infra.grpc.Direction protoDirection) {
        switch (protoDirection) {
        case FIRST:
            return Direction.FIRST;
        case SECOND:
            return Direction.SECOND;
        default:
            throw new IllegalArgumentException("Unknown the direction type '" + protoDirection + '\'');
        }
    }

    public static Instant toInstant(TimestampOrBuilder timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
