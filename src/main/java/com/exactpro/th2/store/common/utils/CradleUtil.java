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

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.schema.cradle.CradleConfiguration;

public class CradleUtil {

    /**
     * @deprecated please use {@link #createCradleManager(CradleConfiguration, String)}
     */
    @Deprecated
    public static CradleManager createCradleManager(CradleConfiguration cradleConfiguration) {
        try {
            return createCradleManager(cradleConfiguration, null);
        } catch (CradleStorageException e) {
            throw new IllegalStateException("Cradle manger initialisation filed", e);
        }
    }

    public static CradleManager createCradleManager(CradleConfiguration cradleConfiguration, String instanceName) throws CradleStorageException {
        CassandraConnectionSettings cassandraConnectionSettings = new CassandraConnectionSettings(
                cradleConfiguration.getDataCenter(),
                cradleConfiguration.getHost(),
                cradleConfiguration.getPort(),
                cradleConfiguration.getKeyspace());

        if (StringUtils.isNotEmpty(cradleConfiguration.getUsername())) {
            cassandraConnectionSettings.setUsername(cradleConfiguration.getUsername());
        }

        if (StringUtils.isNotEmpty(cradleConfiguration.getPassword())) {
            cassandraConnectionSettings.setPassword(cradleConfiguration.getPassword());
        }

        CassandraCradleManager cassandraCradleManager = new CassandraCradleManager(new CassandraConnection(cassandraConnectionSettings));
        cassandraCradleManager.init(ObjectUtils.defaultIfNull(instanceName, "unknown"));
        return cassandraCradleManager;
    }
}
