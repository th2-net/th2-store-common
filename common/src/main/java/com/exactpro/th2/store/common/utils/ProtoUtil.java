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
package com.exactpro.th2.store.common.utils;

import com.exactpro.cradle.messages.StoredMessageBatchId;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEvent;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.testevents.StoredTestEventBatchId;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.eventstore.grpc.EventID;
import com.exactpro.th2.infra.grpc.MessageID;

public class ProtoUtil {

    public static StoredTestEventId toStoredTestEventId(EventID eventId) {
        return new StoredTestEventId(new StoredTestEventBatchId(eventId.getBatchId().getId()),
                (int)eventId.getIndex());
    }

    public static StoredTestEventBatch toBatch(StoredTestEvent event) throws CradleStorageException {
        return StoredTestEventBatch.singleton(event);
    }

    //FIXME: This method doesn't work because TH2 API migrated to sequence
    public static StoredMessageId toStoredMessageId(MessageID messageId) {
        return new StoredMessageId(new StoredMessageBatchId(String.valueOf(messageId.getSequence())),
                (int)messageId.getSequence()); //FIXME: This is stub
    }
}
