#!/usr/bin/env bash

set -o pipefail
set -e

SPARK_HOME="$(cd "`dirname "$0"`/.."; pwd)"
MVN="$SPARK_HOME/build/mvn"

VERSION=$("$MVN" help:evaluate -Dexpression=project.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)

SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.binary.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | grep -v "WARNING"\
    | tail -n 1)

CODE_VERSION=`echo $VERSION | sed -e 's/-SNAPSHOT$//g'`

echo releases_$SCALA_VERSION/$CODE_VERSION
