package com.vmware.dcm;


import com.vmware.dcm.generated.parser.DcmSqlParserImpl;
import com.vmware.dcm.parser.SqlCreateConstraint;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.jooq.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


public class AutoScope {
    private static final Logger LOG = LoggerFactory.getLogger(AutoScope.class);

    private final DSLContext conn;
    public final List<String> constraints;

    AutoScope(final DSLContext conn, final List<String> constraints) {
        this.conn = conn;
        this.constraints = constraints;
        parseConstraint(conn, constraints);
    }

    public void scope() {
    }

    private static void parseConstraint(final DSLContext dslContext, final List<String> constraints) {
        final List<SqlCreateConstraint> constraintViews = constraints.stream().map(
                constraint -> {
                    try {
                        final SqlParser.Config config = SqlParser.config()
                                .withParserFactory(DcmSqlParserImpl.FACTORY)
                                .withConformance(SqlConformanceEnum.LENIENT);
                        final SqlParser parser = SqlParser.create(constraint, config);
                        return (SqlCreateConstraint) parser.parseStmt();
                    } catch (final SqlParseException e) {
                        LOG.error("Could not parse view: {}", constraint, e);
                        throw new ModelException(e);
                    }
                }
        ).collect(Collectors.toList());

        final Map<String, String> selectClause = new HashMap<>();
        final ExtractConstraintInQuery visitor = new ExtractConstraintInQuery(selectClause);
        for (SqlCreateConstraint view : constraintViews) {
            visitor.visit(view);
        }
        LOG.info(selectClause.toString());
    }


}
