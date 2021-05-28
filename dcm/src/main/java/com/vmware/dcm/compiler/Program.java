/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A program is a representation of the set of views passed to a model.
 */
public class Program<T> {
    private final Map<String, T> nonConstraintViews;
    private final Map<String, T> constraintViews;
    private final Map<String, T> objectiveFunctionViews;

    Program() {
        nonConstraintViews = new LinkedHashMap<>();
        constraintViews = new LinkedHashMap<>();
        objectiveFunctionViews = new LinkedHashMap<>();
    }

    Program(final Map<String, T> nonConstraintViews, final Map<String, T> constraintViews,
            final Map<String, T> objectiveFunctionViews) {
        this.nonConstraintViews = nonConstraintViews;
        this.constraintViews = constraintViews;
        this.objectiveFunctionViews = objectiveFunctionViews;
    }

    public Map<String, T> nonConstraintViews() {
        return nonConstraintViews;
    }

    public Map<String, T> objectiveFunctionViews() {
        return objectiveFunctionViews;
    }

    public Map<String, T> constraintViews() {
        return constraintViews;
    }

    public <R> Program<R> transformWith(final Function<T, R> function) {
        final Function<Map<String, T>, Map<String, R>> mapFunction = (inputMap) -> inputMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> function.apply(entry.getValue())));
        return new Program<>(mapFunction.apply(nonConstraintViews), mapFunction.apply(constraintViews),
                             mapFunction.apply(objectiveFunctionViews));
    }

    public void forEachNonConstraint(final BiConsumer<? super String, ? super T> action) {
        nonConstraintViews.forEach(action);
    }

    public void forEach(final BiConsumer<? super String, ? super T> action) {
        nonConstraintViews.forEach(action);
        constraintViews.forEach(action);
        objectiveFunctionViews.forEach(action);
    }

    public void forEach(final BiConsumer<? super String, ? super T> nonConstraintViewAction,
                        final BiConsumer<? super String, ? super T> constraintViewAction,
                        final BiConsumer<? super String, ? super T> objectiveViewAction) {
        nonConstraintViews.forEach(nonConstraintViewAction);
        constraintViews.forEach(constraintViewAction);
        objectiveFunctionViews.forEach(objectiveViewAction);
    }
}