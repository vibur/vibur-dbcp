/**
 * Copyright 2014 Daniel Caldeweyher
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

package org.vibur.dbcp;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;

/**
 * A JNDI factory that produces ViburDBCPDataSource instances.
 *
 * @author Daniel Caldeweyher
 */
@SuppressWarnings("unused")
public class ViburDBCPObjectFactory implements ObjectFactory {

    @Override
    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment)
        throws ViburDBCPException {

        Reference reference = (Reference) obj;
        Enumeration<RefAddr> enumeration = reference.getAll();
        Properties props = new Properties();
        while (enumeration.hasMoreElements()) {
            RefAddr refAddr = enumeration.nextElement();
            String pname = refAddr.getType();
            String pvalue = (String) refAddr.getContent();
            props.put(pname, pvalue);
        }

        ViburDBCPDataSource dataSource = new ViburDBCPDataSource(props);
        dataSource.start();
        return dataSource;
    }
}
