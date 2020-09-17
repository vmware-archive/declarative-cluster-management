/*
 * Copyright © 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler.monoid;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class MonoidComprehension extends Expr {
    @Nullable private final Head head;
    private final List<Qualifier> qualifiers;

    public MonoidComprehension(final Head head) {
        this.head = head;
        this.qualifiers = new ArrayList<>();
    }

    public MonoidComprehension(final Head head, final List<Qualifier> qualifiers) {
        this.head = head;
        this.qualifiers = qualifiers;
    }

    public MonoidComprehension() {
        this.head = null;
        this.qualifiers = Collections.emptyList();
    }

    public MonoidComprehension withQualifier(final Qualifier qualifier) {
        final List<Qualifier> newQualifiers = new ArrayList<>(qualifiers);
        newQualifiers.add(qualifier);
        return new MonoidComprehension(Objects.requireNonNull(head), newQualifiers);
    }

    public Head getHead() {
        return Objects.requireNonNull(head);
    }

    public List<Qualifier> getQualifiers() {
        return qualifiers;
    }

    @Override
    public String toString() {
        return  String.format("[%s | %s]", head, qualifiers);
    }

    @Override
    <T, C> T acceptVisitor(final MonoidVisitor<T, C> visitor, final C context) {
        return visitor.visitMonoidComprehension(this, context);
    }
}