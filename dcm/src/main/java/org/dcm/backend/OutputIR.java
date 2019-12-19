/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import com.squareup.javapoet.CodeBlock;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A simple IR to represent the output Java program that we are generating
 * for the OrTools solver. The IR represents a tree of "blocks", where each block has
 * a set of variable declarations, headers and a body comprised of more Blocks.
 * Headers for the time being are just strings.
 *
 * toString() recursively generates the required code.
 */
class OutputIR {

    /**
     * Base for every type of expression, identified by a non-unique name
     */
    static class BlockExpr {
        private final String name;

        BlockExpr(final String name) {
            this.name = name;
        }

        protected String getName() {
            return name;
        }
    }

    /**
     * A generic block of code, that starts with a header of strings, followed by a list of blocks. Each
     * block has a set of variable declarations.
     */
    static class Block extends BlockExpr {
        private final Set<StringBlock> header;
        private final List<BlockExpr> children;
        private final Declarations declarations;
        private final List<BlockExpr> insertionOrder = new ArrayList<>();

        Block(final String name) {
            super(name);
            this.children = new ArrayList<>();
            this.header = new LinkedHashSet<>();
            this.declarations = new Declarations(name);
        }

        boolean addHeader(final CodeBlock blockExpr) {
            final StringBlock b = new StringBlock(blockExpr);
            return header.add(b);
        }

        void addChild(final Block child) {
            children.add(child);
            insertionOrder.add(child);
        }

        void addChild(final CodeBlock child) {
            final StringBlock b = new StringBlock(child);
            insertionOrder.add(b);
        }

        String declare(final String expr) {
            final String varName = declarations.add(expr);
            insertionOrder.add(new DeclarationStatement(varName, expr));
            return varName;
        }

        boolean hasDeclaration(final String expr) {
            return declarations.exists(expr);
        }

        String getDeclaredName(final String expr) {
            return declarations.get(expr);
        }

        public List<BlockExpr> getForLoopsByName(final String name) {
            return children.stream()
                    .filter(e ->  e instanceof OutputIR.ForBlock &&  e.getName().equals(name))
                    .collect(Collectors.toList());
        }

        @Override
        public String toString() {
            final CodeBlock.Builder b = CodeBlock.builder();
            header.forEach(
                    e -> b.add(e.toString())
            );
            insertionOrder.forEach(
                    e -> {
                        b.add(e.toString());
                    }
            );
            return b.build().toString();
        }
    }

    /**
     * A block representing a nested for loop.
     */
    static class ForBlock extends Block {
        private final List<CodeBlock> loopExpr;

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

    /**
     * A block representing an If statement.
     */
    static class IfBlock extends Block {
        private final String predicate;

        IfBlock(final String name, final String predicate) {
            super(name);
            this.predicate = predicate;
        }

        @Override
        public String toString() {
            final CodeBlock.Builder b = CodeBlock.builder();
            b.beginControlFlow(predicate);
            b.addStatement("continue");
            b.endControlFlow();
            return b.build().toString();
        }
    }

    /**
     * Tracks the set of variable declarations within a block. Within a given block, expressions
     * are reused if possible.
     */
    static class Declarations extends BlockExpr {
        private static final String TEMP_VAR_PREFIX = "i";
        private final Map<String, List<String>> declarations = new LinkedHashMap<>();
        private static final AtomicInteger VAR_COUNTER = new AtomicInteger(0);

        Declarations(final String name) {
            super(name);
        }

        boolean exists(final String expression) {
            return declarations.containsKey(expression);
        }

        String get(final String expression) {
            return declarations.get(expression).get(0);
        }

        String add(final String expression) {
            final String varName = TEMP_VAR_PREFIX + VAR_COUNTER.getAndIncrement();
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

    /**
     * Tracks the actual statements within a block where an expression has to be assigned to a variable
     */
    static class DeclarationStatement extends BlockExpr {
        private final String expr;
        private final String varName;

        DeclarationStatement(final String varName, final String expr) {
            super("declaration");
            this.varName = varName;
            this.expr = expr;
        }

        @Override
        public String toString() {
            return CodeBlock.builder().addStatement("var $L = $L", varName, expr).build().toString();
        }
    }

    /**
     * A block of code, already in the form of the output string. Used for boilerplate within the generated code
     * like comments and some initialization statements.
     */
    static class StringBlock extends BlockExpr {
        private final CodeBlock codeBlock;

        StringBlock(final CodeBlock codeBlock) {
            super("string-block");
            this.codeBlock = codeBlock;
        }

        @Override
        public String toString() {
            return codeBlock.toString();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StringBlock)) {
                return false;
            }
            final StringBlock that = (StringBlock) o;
            return codeBlock.equals(that.codeBlock);
        }

        @Override
        public int hashCode() {
            return Objects.hash(codeBlock);
        }
    }
}