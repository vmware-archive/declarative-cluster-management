/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm.compiler;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A program is a representation of the set of views passed to a model. It hosts three types of views.
 * - Constraint views, which are views with a check clause that represent hard constraints.
 * - Objective views, which are views representing objective functions or soft constraints
 * - Non constraint views, which are views that compute intermediary results for the other two views
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

    /**
     * Apply the supplied function to all views represented by this program and return a new program.
     *
     * @param function A function that takes an instance of a view of type T and transforms it into a view of type R
     * @param <R> New type for target program
     * @return A Program of type R, after applying the supplied function to the current Program of type T
     */
    public <R> Program<R> map(final Function<T, R> function) {
        final BiFunction<String, T, R> biFunction = (name, comprehension) -> function.apply(comprehension);
        return new Program<>(toMapFunction(biFunction).apply(nonConstraintViews),
                             toMapFunction(biFunction).apply(constraintViews),
                             toMapFunction(biFunction).apply(objectiveFunctionViews));
    }

    /**
     * Apply the supplied bi-function to all views represented by this program and return a new program.
     *
     * @param function A bi-function that takes the name of view, and an instance of the view's type T and
     *                 transforms it into a view of type R
     * @param <R> New type for target program
     * @return A Program of type R, after applying the supplied bi-function to the current Program of type T
     */
    public <R> Program<R> map(final BiFunction<String, T, R> function) {
        return new Program<>(toMapFunction(function).apply(nonConstraintViews),
                             toMapFunction(function).apply(constraintViews),
                             toMapFunction(function).apply(objectiveFunctionViews));
    }

    /**
     * Apply the supplied bi-functions to each of the three types of views represented by this program and
     * return a new program.
     *
     * @param nonConstraintViewAction A bi-function that takes the name of view, and an instance of a
     *                                non-constraint view of type T and transforms it into a view of type R
     * @param constraintViewAction A bi-function that takes the name of view, and an instance of a
     *                             constraint view of type T and transforms it into a view of type R
     * @param objectiveViewAction A bi-function that takes the name of view, and an instance of an
     *                            objective view of type T and transforms it into a view of type R
     * @param <R> New type for target program
     * @return A Program of type R, after applying the supplied bi-functions to the current Program of type T
     */
    public <R> Program<R> map(final BiFunction<String, T, R> nonConstraintViewAction,
                              final BiFunction<String, T, R> constraintViewAction,
                              final BiFunction<String, T, R> objectiveViewAction) {
        return new Program<>(toMapFunction(nonConstraintViewAction).apply(nonConstraintViews),
                             toMapFunction(constraintViewAction).apply(constraintViews),
                             toMapFunction(objectiveViewAction).apply(objectiveFunctionViews));
    }

    /**
     * Apply the supplied function to all views represented by this program.
     *
     * @param action An action that executes per instance of a view of type T
     */
    public void forEach(final BiConsumer<String, T> action) {
        nonConstraintViews.forEach(action);
        constraintViews.forEach(action);
        objectiveFunctionViews.forEach(action);
    }

    /**
     * Apply the supplied function to all views represented by this program.
     *
     * @param nonConstraintViewAction An action that executes per instance of a non-constraint view of type T
     * @param constraintViewAction An action that executes per instance of a constraint view of type T
     * @param objectiveViewAction An action that executes per instance of a objective view of type T
     */
    public void forEach(final BiConsumer<String, T> nonConstraintViewAction,
                        final BiConsumer<String, T> constraintViewAction,
                        final BiConsumer<String, T> objectiveViewAction) {
        nonConstraintViews.forEach(nonConstraintViewAction);
        constraintViews.forEach(constraintViewAction);
        objectiveFunctionViews.forEach(objectiveViewAction);
    }

    private <R> Function<Map<String, T>, Map<String, R>> toMapFunction(final BiFunction<String, T, R> function) {
        return (inputMap) -> inputMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> function.apply(entry.getKey(), entry.getValue())));
    }
}