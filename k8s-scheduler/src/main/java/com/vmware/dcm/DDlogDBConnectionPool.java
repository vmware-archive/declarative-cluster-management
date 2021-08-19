package com.vmware.dcm;

import com.vmware.ddlog.DDlogJooqProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamCastMode;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;

public class DDlogDBConnectionPool implements IConnectionPool {

    private final DDlogJooqProvider provider;

    public DDlogDBConnectionPool(DDlogJooqProvider provider) {
        this.provider = provider;
    }

    @Override
    public DSLContext getConnectionToDb() {
        final MockConnection connection = new MockConnection(provider);
        return DSL.using(connection, SQLDialect.H2, new Settings()
                .withExecuteLogging(false)
                .withParamCastMode(ParamCastMode.NEVER));
    }

    @Override
    public DSLContext getDataConnectionToDb() {
        return provider.getDslContext();
    }
}
