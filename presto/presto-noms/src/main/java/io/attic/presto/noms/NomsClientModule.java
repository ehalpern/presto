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
package io.attic.presto.noms;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Guice module for binding the noms plugin implementation.
 */
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
        binder.bind(NomsPageSourceProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(NomsClientConfig.class);
    }

    /**
     * Custom factory for NomsSession
     */
    @Singleton
    @Provides
    public static NomsSession createNomsSession(
            NomsConnectorId connectorId,
            NomsClientConfig config)
    {
        return new NomsSession(connectorId.toString(), config);
    }
}
