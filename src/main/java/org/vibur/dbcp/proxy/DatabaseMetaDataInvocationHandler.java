/**
 * Copyright 2016 Simeon Malchev
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

package org.vibur.dbcp.proxy;

import org.vibur.dbcp.ViburConfig;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;

/**
 * @author Simeon Malchev
 */
class DatabaseMetaDataInvocationHandler extends ChildObjectInvocationHandler<Connection, DatabaseMetaData> {

    DatabaseMetaDataInvocationHandler(DatabaseMetaData rawMetaData, Connection connProxy,
                                      ViburConfig config, ExceptionCollector exceptionCollector) {
        super(rawMetaData, connProxy, "getConnection", config, exceptionCollector);
    }

    @Override
    Object doInvoke(DatabaseMetaData proxy, Method method, Object[] args) throws Throwable {
        databaseAccessDoorway(proxy, method, args);
        return super.doInvoke(proxy, method, args);
    }
}
