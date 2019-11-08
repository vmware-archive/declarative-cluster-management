#!/bin/bash

set -ex
cwd=$(pwd)

JDK_OS=darwin
JAVA_HOME=~/.jenv/versions/12.0
DDLOG=~/Documents/DCM/ddlog-0.10.3
DPROG=dcm_opt
FLATBUFFERS_JAR_PATH=~/Documents/DCM/flatbuffers-java-1.11.0.jar
CLASSPATH=${DDLOG}/java/ddlogapi.jar:.:${FLATBUFFERS_JAR_PATH}:$CLASSPATH
DDLOG_MODEL_ENV=${DDLOG}/${DPROG}_ddlog/libddlogapi.dylib

cp ${DPROG}.dl ${DDLOG}
cd ${DDLOG}
bin/ddlog -i ${DPROG}.dl -L lib -j
cp Cargo.lock ${DPROG}_ddlog/
cd ${DPROG}_ddlog
cargo build --features=flatbuf --release

cc -shared -fPIC -I${JAVA_HOME}/include -I${JAVA_HOME}/include/${JDK_OS} -I. -I${DDLOG}/lib ${DDLOG}/java/ddlogapi.c -Ltarget/release/ -l${DPROG}_ddlog -o libddlogapi.dylib

cd ${DDLOG}/${DPROG}_ddlog/flatbuf/java/
javac ddlog/__${DPROG}/*.java
javac ddlog/${DPROG}/*.java

jar -cf weave-apps.jar ddlog/*

mvn install:install-file -Dfile=weave-apps.jar -DgroupId=ddlog.${DPROG} -DartifactId=ddlog.${DPROG} -Dversion=0.1 -Dpackaging=jar
mvn install:install-file -Dfile=${DDLOG}/java/ddlogapi.jar -DgroupId=ddlogapi -DartifactId=ddlog -Dversion=1.0 -Dpackaging=jar

cd $cwd
mvn clean package -DskipTests
cd benchmarks/target
mkdir resources
java -cp benchmarks.jar -Djava.library.path="${DDLOG}/${DPROG}_ddlog" org.dcm.DBBenchmark

