/*
 *
 *  * Copyright © 2017 - 2018 VMware, Inc. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file
 *  * except in compliance with the License. You may obtain a copy of the License at
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed under the
 *  * License is distributed on an “AS IS” BASIS, without warranties or conditions of any kind,
 *  * EITHER EXPRESS OR IMPLIED. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
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
