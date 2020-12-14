/*
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
package com.facebook.presto.hive.security.ranger;

import com.amazonaws.util.StringUtils;
import com.facebook.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.io.File;
import java.net.MalformedURLException;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public class RangerAuthorizer
{
    private static final Logger LOG = Logger.get(RangerAuthorizer.class);
    private final RangerBasePlugin plugin;
    public static final String CLUSTER_NAME = "Presto";
    public static final String HIVE = "hive";

    public RangerAuthorizer(ServicePolicies servicePolicies, RangerBasedAccessControlConfig rangerBasedAccessControlConfig)
    {
        RangerPolicyEngineOptions rangerPolicyEngineOptions = new RangerPolicyEngineOptions();
        Configuration conf = new Configuration();
        rangerPolicyEngineOptions.configureDefaultRangerAdmin(conf, "hive");
        RangerPluginConfig rangerPluginConfig = new RangerPluginConfig(HIVE, rangerBasedAccessControlConfig.getRangerHiveServiceName(), HIVE, CLUSTER_NAME, null,
                rangerPolicyEngineOptions);
        plugin = new RangerBasePlugin(rangerPluginConfig);

        String hiveAuditPath = rangerBasedAccessControlConfig.getRangerHiveAuditPath();
        if (!StringUtils.isNullOrEmpty(hiveAuditPath)) {
            try {
                plugin.getConfig().addResource(new File(hiveAuditPath).toURI().toURL());
            }
            catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }

        AuditProviderFactory providerFactory = AuditProviderFactory.getInstance();

        if (!providerFactory.isInitDone()) {
            if (plugin.getConfig().getProperties() != null) {
                providerFactory.init(plugin.getConfig().getProperties(), HIVE);
            }
            else {
                LOG.info("Audit subsystem is not initialized correctly. Please check audit configuration. ");
                LOG.info("No authorization audits will be generated. ");
            }
        }

        plugin.setResultProcessor(new RangerDefaultAuditHandler());
        plugin.setPolicies(servicePolicies);
    }

    public boolean authorizeHiveResource(String database, String table, String column, String accessType, String user, Set<String> userGroups, Set<String> userRoles)
    {
        final String keyDatabase = "database";
        final String keyTable = "table";
        final String keyColumn = "column";

        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        if (!StringUtils.isNullOrEmpty(database)) {
            resource.setValue(keyDatabase, database);
        }

        if (!StringUtils.isNullOrEmpty(table)) {
            resource.setValue(keyTable, table);
        }

        if (!StringUtils.isNullOrEmpty(column)) {
            resource.setValue(keyColumn, column);
        }

        RangerAccessRequest request = new RangerAccessRequestImpl(resource, accessType.toLowerCase(ENGLISH), user, userGroups, userRoles);

        RangerAccessResult result = plugin.isAccessAllowed(request);

        return result != null && result.getIsAllowed();
    }
}
