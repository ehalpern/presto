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
package com.facebook.presto.noms;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class NomsClientModule
        implements Module
{
    private final String connectorId;

    public NomsClientModule(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(NomsConnectorId.class).toInstance(new NomsConnectorId(connectorId));
        binder.bind(NomsConnector.class).in(Scopes.SINGLETON);
        binder.bind(NomsMetadata.class).in(Scopes.SINGLETON);
        binder.bind(NomsSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(NomsRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(NomsConnectorRecordSinkProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(NomsClientConfig.class);
    }

    @Singleton
    @Provides
    public static NomsSession createNomsSession(
            NomsConnectorId connectorId,
            NomsClientConfig config)
    {
        requireNonNull(config, "config is null");
        return new NativeNomsSession(connectorId.toString(), config);
    }
}
