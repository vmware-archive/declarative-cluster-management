/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */
package org.dcm.backend;

import com.squareup.javapoet.CodeBlock;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

class CodeTree {

    static class BlockExpr {

    }

    static class Block extends BlockExpr {
        final List<CodeBlock> header;
        final List<Block> children;
        final List<CodeBlock> trailer;
        final Declarations declarations;
        final String name;
        final Set<String> tailDeclarations;

        Block(final String name) {
            this.children = new ArrayList<>();
            this.header = new ArrayList<>();
            this.trailer = new ArrayList<>();
            this.declarations = new Declarations(name);
            this.name = name;
            this.tailDeclarations = new HashSet<>();
        }

        void addHeader(final CodeBlock blockExpr) {
            header.add(blockExpr);
        }

        void addChild(final Block child) {
            children.add(child);
        }

        void addTrailer(final CodeBlock blockExpr) {
            trailer.add(blockExpr);
        }

        String declare(final String expr, final boolean tail) {
            if (tail) {
                tailDeclarations.add(expr);
            }
            return declarations.add(expr);
        }

        boolean hasDeclaration(final String expr) {
            return declarations.exists(expr);
        }

        String getDeclaredName(final String expr) {
            return declarations.get(expr);
        }

        @Override
        public String toString() {
            final CodeBlock.Builder b = CodeBlock.builder();
            header.forEach(
                    e -> b.add(e)
            );

            declarations.declarations.forEach(
                    (k, v) -> {
                        if (!tailDeclarations.contains(k)) {
                            b.addStatement("var $L = $L", v.get(0), k);
                        }
                    }
            );

            children.forEach(
                    e -> b.add(e.toString())
            );

            declarations.declarations.forEach(
                    (k, v) -> {
                        if (tailDeclarations.contains(k)) {
                            b.addStatement("var $L = $L", v.get(0), k);
                        }
                    }
            );

            trailer.forEach(e -> b.add(e));
            return b.build().toString();
        }
    }

    static class ForBlock extends Block {
        final List<CodeBlock> loopExpr;

        ForBlock(final String name, final CodeBlock loopExpr) {
            super(name);
            this.loopExpr = List.of(loopExpr);
        }

        ForBlock(final String name, final List<CodeBlock> loopExpr) {
            super(name);
            this.loopExpr = loopExpr;
        }

        @Override
        public String toString() {
            final CodeBlock.Builder b = CodeBlock.builder();
            loopExpr.forEach(
                    e -> b.beginControlFlow(e.toString())
            );
            final String s = super.toString();
            b.add(s);
            loopExpr.forEach(e -> b.endControlFlow());
            return b.build().toString();
        }
    }

    static class IfBlock extends Block {
        final String predicate;

        IfBlock(final String name, final String predicate) {
            super(name);
            this.predicate = predicate;
        }

        @Override
        public String toString() {
            final CodeBlock.Builder b = CodeBlock.builder();
            b.beginControlFlow(predicate);
            b.add(super.toString());
            b.endControlFlow();
            return b.build().toString();
        }
    }

    static class Declarations extends BlockExpr {
        private static final String TEMP_VAR_PREFIX = "i";
        private final Map<String, List<String>> declarations = new LinkedHashMap<>();
        private static final AtomicInteger varCounter = new AtomicInteger(0);

        Declarations(final String scopePrefix) {
//            this.scopePrefix = scopePrefix;
        }

        boolean exists(final String expression) {
            return declarations.containsKey(expression);
        }

        String get(final String expression) {
            return declarations.get(expression).get(0);
        }

        String add(final String expression) {
//            final String varName = TEMP_VAR_PREFIX + "S" + scopePrefix + "V" + varCounter.getAndIncrement();
            final String varName = TEMP_VAR_PREFIX + varCounter.getAndIncrement();
            declarations.computeIfAbsent(expression,
                                         (k) -> new ArrayList<>()).add(varName);
            return declarations.get(expression).get(0);
        }

        @Override
        public String toString() {
            return "Declarations{" +
                    "declarations=" + declarations +
                    '}';
        }
    }

    static class TupleCollect extends BlockExpr {
        private final String name;

        TupleCollect(final String name) {
            this.name = name;
        }
    }

    static class ListCollect extends BlockExpr {
        private final String name;

        ListCollect(final String name) {
            this.name = name;
        }
    }
}
