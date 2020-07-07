/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import org.dcm.compiler.monoid.MonoidLiteral;
import org.dcm.compiler.monoid.VoidType;
import org.dcm.compiler.monoid.SimpleVisitor;

import java.util.HashSet;
import java.util.Set;

public class FindStringLiterals extends SimpleVisitor {
    private final Set<String> stringLiterals = new HashSet<>();

    @Override
    protected VoidType visitMonoidLiteral(final MonoidLiteral node, final VoidType context) {
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
