/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import org.dcm.compiler.monoid.Qualifier;

import java.util.ArrayList;
import java.util.List;

/**
 * Container to propagate var and non-var qualifiers
 */
class QualifiersList {
    private final List<Qualifier> varQualifiers;
    private final List<Qualifier> nonVarQualifiers;

    QualifiersList() {
        this.varQualifiers = new ArrayList<>();
        this.nonVarQualifiers = new ArrayList<>();
    }

    QualifiersList(final List<Qualifier> varQualifiers, final List<Qualifier> nonVarQualifiers) {
        this.varQualifiers = new ArrayList<>(varQualifiers);
        this.nonVarQualifiers = new ArrayList<>(nonVarQualifiers);
    }

    QualifiersList withNonVarQualifier(final Qualifier qualifier) {
        final List<Qualifier> newNonVarQualifiers = new ArrayList<>(nonVarQualifiers);
        newNonVarQualifiers.add(qualifier);
        return new QualifiersList(varQualifiers, newNonVarQualifiers);
    }


    QualifiersList withVarQualifier(final Qualifier qualifier) {
        final List<Qualifier> newVarQualifiers = new ArrayList<>(varQualifiers);
        newVarQualifiers.add(qualifier);
        return new QualifiersList(newVarQualifiers, nonVarQualifiers);
    }

    QualifiersList withQualifiersList(final QualifiersList in) {
        final List<Qualifier> newNonVarQualifiers = new ArrayList<>(this.nonVarQualifiers);
        newNonVarQualifiers.addAll(in.nonVarQualifiers);
        final List<Qualifier> newVarQualifiers = new ArrayList<>(this.varQualifiers);
        newVarQualifiers.addAll(in.varQualifiers);
        return new QualifiersList(newVarQualifiers, newNonVarQualifiers);
    }

    public List<Qualifier> getVarQualifiers() {
        return varQualifiers;
    }

    public List<Qualifier> getNonVarQualifiers() {
        return nonVarQualifiers;
    }

    @Override
    public String toString() {
        return String.format("(Var: %s, NonVar: %s)", varQualifiers, nonVarQualifiers);
    }
}
