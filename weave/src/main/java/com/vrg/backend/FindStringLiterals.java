/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg.backend;

import com.vrg.compiler.monoid.MonoidLiteral;
import com.vrg.compiler.monoid.MonoidVisitor;

import java.util.HashSet;
import java.util.Set;

public class FindStringLiterals extends MonoidVisitor<Void, Void> {
    private final Set<String> stringLiterals = new HashSet<>();

    @Override
    protected Void visitMonoidLiteral(final MonoidLiteral node, final Void context) {
        if (node.getValue() instanceof String) {
            final String s = node.getValue().toString();
            if (s.startsWith("'") && s.endsWith("'")) {
                stringLiterals.add(node.getValue().toString());
            }
        }
        return super.visitMonoidLiteral(node, context);
    }

    Set<String> getStringLiterals() {
        return stringLiterals;
    }
}
