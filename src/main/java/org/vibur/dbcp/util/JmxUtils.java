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

package org.vibur.dbcp.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vibur.dbcp.ViburDBCPMonitoringMBean;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * @author Simeon Malchev
 */
public final class JmxUtils {

    private static final Logger logger = LoggerFactory.getLogger(JmxUtils.class);

    private JmxUtils() {}

    public static void registerMBean(ViburDBCPMonitoringMBean mBean, String jmxName) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName(jmxName);
            if (!mbs.isRegistered(objectName))
                mbs.registerMBean(mBean, objectName);
            else
                logger.warn(jmxName + " is already registered.");
        } catch (JMException e) {
            logger.warn("Unable to register mBean {}", jmxName, e);
        }
    }

    public static void unregisterMBean(String jmxName) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName(jmxName);
            if (mbs.isRegistered(objectName))
                mbs.unregisterMBean(objectName);
            else
                logger.warn(jmxName + " is not registered.");
        } catch (JMException e) {
            logger.warn("Unable to unregister mBean {}", jmxName, e);
        }
    }
}
