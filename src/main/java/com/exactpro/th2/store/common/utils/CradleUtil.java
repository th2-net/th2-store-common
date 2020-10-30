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

import org.apache.commons.lang3.StringUtils;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.cassandra.connection.CassandraConnectionSettings;
import com.exactpro.th2.common.schema.cradle.CradleConfiguration;

public class CradleUtil {

    public static CradleManager createCradleManager(CradleConfiguration cradleConfiguration) {
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

        return new CassandraCradleManager(new CassandraConnection(cassandraConnectionSettings));
    }
}
