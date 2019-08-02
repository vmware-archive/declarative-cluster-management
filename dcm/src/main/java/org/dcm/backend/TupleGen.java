/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package org.dcm.backend;

import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;

import javax.lang.model.element.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class TupleGen {
    private static final Map<Integer, TypeSpec> TUPLES = new ConcurrentHashMap<>();

    static Collection<TypeSpec> getAllTupleTypes() {
        return TUPLES.values();
    }

    static TypeSpec getTupleType(final int numRecords) {
        return TUPLES.computeIfAbsent(numRecords, TupleGen::tupleGen);
    }

    /**
     * Create a tuple type with 'numFields' entries. Results in a generic "plain old java object"
     * with a getter per field.
     */
    static TypeSpec tupleGen(final int numFields) {
        final TypeSpec.Builder classBuilder = TypeSpec.classBuilder("Tuple" + numFields)
                .addModifiers(Modifier.FINAL, Modifier.PRIVATE);
        final MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PRIVATE);
        for (int i = 0; i < numFields; i++) {
            final TypeVariableName type = TypeVariableName.get("T" + i);
            // Add parameter to constructor
            constructor.addParameter(type, "t" + i, Modifier.FINAL)
                    .addStatement("this.$1L = $1L", "t" + i); // assign parameters to fields

            // Create getter
            final MethodSpec getter = MethodSpec.methodBuilder("value" + i)
                    .returns(type)
                    .addStatement("return t" + i)
                    .build();

            // Add field and getter to class
            classBuilder.addTypeVariable(type)
                    .addField(type, "t" + i, Modifier.PRIVATE, Modifier.FINAL)
                    .addMethod(getter);
        }

        final String toPrint = IntStream.range(0, numFields)
                .mapToObj(i -> "t" + i)
                .collect(Collectors.joining(", "));
        final MethodSpec toStringMethod = MethodSpec.methodBuilder("toString")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(String.class)
                .addStatement("return String.format($S, $L)", "(%s)", toPrint)
                .build();
        final MethodSpec hashCodeMethod = MethodSpec.methodBuilder("hashCode")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(int.class)
                .addStatement("return this.toString().hashCode()")
                .build();
        final MethodSpec.Builder equals = MethodSpec.methodBuilder("equals")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(boolean.class)
                .addParameter(Object.class, "other", Modifier.FINAL)
                .beginControlFlow("if (other == this)")
                .addStatement("return true")
                .endControlFlow()
                .beginControlFlow("if (!(other instanceof Tuple$L))", numFields)
                .addStatement("return false")
                .endControlFlow()
                .addStatement("final Tuple$1L that = (Tuple$1L) other", numFields)
                .addCode("return ");

        final String returnValue = IntStream.range(0, numFields)
                .mapToObj(i -> String.format("this.value%s().equals(that.value%s())", i, i))
                .collect(Collectors.joining(" && "));
        equals.addCode("$L;\n", returnValue);
        final MethodSpec equalsMethod = equals.build();
        return classBuilder.addMethod(constructor.build())
                .addMethod(toStringMethod)
                .addMethod(hashCodeMethod)
                .addMethod(equalsMethod)
                .build();
    }
}
