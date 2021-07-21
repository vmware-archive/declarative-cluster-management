<#--
// Copyright 2018-2021 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: BSD-2
-->

SqlCreate SqlCreateConstraint(Span s, boolean replace): {
    SqlIdentifier constraintName;
    SqlNode query;
    String type;
    SqlNode constraint;
}
{
    <CONSTRAINT>
    constraintName = CompoundIdentifier()
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    (
        (
         <CHECK> {type = "HARD_CONSTRAINT";}
            |
         <MAXIMIZE> {type = "OBJECTIVE";}
            |
         <EOF> {return new SqlCreateConstraint(s.pos(), constraintName, query, "INTERMEDIATE_VIEW", null);}
        )
        constraint = OrderedQueryOrExpr(ExprContext.ACCEPT_NON_QUERY)
        {
            return new SqlCreateConstraint(s.pos(), constraintName, query, type, constraint);
        }
    )
}