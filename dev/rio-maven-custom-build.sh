#!/usr/bin/env bash

set -eux -o pipefail

export PATH=$PATH:$JAVA_HOME/bin

# Figure out where the Spark framework is installed
SPARK_HOME="$(cd "$(dirname "$0")/.."; pwd)"
MVN="$SPARK_HOME/build/mvn"
VALID_SCALA_VERSIONS=(2.12)

function exit_with_usage {
  echo "builder.sh <scala-version 2.12>"
  echo ""
  echo ""
  exit 1
}
function check_scala_version() {
  for i in ${VALID_SCALA_VERSIONS[*]}; do [ "$i" = "$1" ] && return 0; done
  echo "Invalid Scala version: $1. Valid versions: ${VALID_VERSIONS[*]}" 1>&2
  exit 1
}

function execute_command () {
  declare -a COMMAND_WITH_ARGS=("$@")

  # Actually build the jar
  echo -e "\nExecuting: ${COMMAND_WITH_ARGS[*]}"

  "${COMMAND_WITH_ARGS[@]}"
}

##Initialise all variables
SCALA_VERSION="2.12"
IS_RELEASE=0
ADDITIONAL_MAVEN_OPTS=""
ADDITIONAL_MAVEN_PARAMS=""
SKIP_TESTS="true"
SKIP_TESTS_D_PARAM="-DskipTests=true"
SKIP_TEST_PACKAGE="false"
SKIP_TEST_PACKAGE_D_PARAM=""
HADOOP_VERSION=""
HADOOP_VERSION_D_PARAM=""
LOCAL_REPO_DIR="$SPARK_HOME/.dist/local-repo"
# Parse arguments
while (( "$#" )); do
  case $1 in
    --scala)
      SCALA_VERSION="$2"
      shift
      ;;
    --hadoop)
      HADOOP_VERSION="$2"
      HADOOP_VERSION_D_PARAM="-Dhadoop.version=${HADOOP_VERSION}"
      shift
      ;;
    --maven-params)
      ADDITIONAL_MAVEN_PARAMS="$2"
      shift
      ;;
    --maven-opts)
      ADDITIONAL_MAVEN_OPTS="$2"
      shift
      ;;
    --do-not-skip-tests)
      SKIP_TESTS="false"
      SKIP_TESTS_D_PARAM=""
      ;;
    --skip-test-package)
      SKIP_TESTS="true"
      SKIP_TESTS_D_PARAM="-DskipTests=true"
      SKIP_TEST_PACKAGE="true"
      SKIP_TEST_PACKAGE_D_PARAM="-Dmaven.test.skip=${SKIP_TEST_PACKAGE}"
      ;;
    --snapshot)
      IS_RELEASE=0
      ;;
    --release)
      IS_RELEASE=1
      ;;
    *)
      exit_with_usage
      ;;
  esac
  shift
done
echo -e "Variables initialised for this build:"
echo -e "====================================="
echo -e "  SCALA_VERSION=[${SCALA_VERSION}]"
echo -e "  IS_RELEASE=[${IS_RELEASE}]"
echo -e "  ADDITIONAL_MAVEN_OPTS=[${ADDITIONAL_MAVEN_OPTS}]"
echo -e "  ADDITIONAL_MAVEN_PARAMS=[${ADDITIONAL_MAVEN_PARAMS}]"
echo -e "  SKIP_TESTS=[${SKIP_TESTS}]"
echo -e "  SKIP_TESTS_D_PARAM=[${SKIP_TESTS_D_PARAM}]"
echo -e "  SKIP_TEST_PACKAGE=[${SKIP_TEST_PACKAGE}]"
echo -e "  SKIP_TEST_PACKAGE_D_PARAM=[${SKIP_TEST_PACKAGE_D_PARAM}]"
echo -e "  HADOOP_VERSION=[${HADOOP_VERSION}]"
echo -e "  HADOOP_VERSION_D_PARAM=[${HADOOP_VERSION_D_PARAM}]"
echo -e "====================================="

if [ -z "$JAVA_HOME" ]; then
  # Fall back on JAVA_HOME from rpm, if found
  if command -v  rpm; then
    RPM_JAVA_HOME="$(rpm -E %java_home 2>/dev/null)"
    if [ "$RPM_JAVA_HOME" != "%java_home" ]; then
      JAVA_HOME="$RPM_JAVA_HOME"
      echo "No JAVA_HOME set, proceeding with '$JAVA_HOME' learned from rpm"
    fi
  fi
fi

if [ -z "$JAVA_HOME" ]; then
  echo "Error: JAVA_HOME is not set, cannot proceed."
  exit -1
fi

if ! command -v "$MVN" ; then
  echo -e "Could not locate Maven command: '$MVN'."
  echo -e "Specify the Maven command with the --mvn flag"
  exit -1;
fi

##Scala Version Validation
if  [[ $SKIP_TEST_PACKAGE == "true" ]] && [[ $SKIP_TESTS == "false" ]] ; then
  echo -e "Contradicting parameters. Only one of the two options, --do-not-skip-tests and --skip-test-package should be provided"
  exit -1;
fi

cd "$SPARK_HOME"

##Set MAVEN_OPTS
export MAVEN_OPTS="${ADDITIONAL_MAVEN_OPTS} ${SKIP_TEST_PACKAGE_D_PARAM} $SKIP_TESTS_D_PARAM -Dscala-${SCALA_VERSION}=enabled -Dhive-thriftserver=enabled ${HADOOP_VERSION_D_PARAM} -DdeployAtEnd=true -DinstallAtEnd=true"

echo -e "MAVEN_OPTS exported: ${MAVEN_OPTS}"

##Update the maven pom files with the input SCALA_VERSION
execute_command "./dev/change-scala-version.sh" "$SCALA_VERSION" "$@"


##Maven Command Executions
execute_command "$MVN" com.apple.cie.rio:rio-maven-plugin:create-marker "$SKIP_TESTS_D_PARAM" "$@"
if [ $IS_RELEASE -eq 1 ] ; then
  execute_command "$MVN" com.apple.cie.rio:rio-maven-plugin:remove-snapshot org.codehaus.mojo:versions-maven-plugin:set "$@"
fi

mkdir -p "${LOCAL_REPO_DIR}"
REPO_URL="local-release::default::file://${LOCAL_REPO_DIR}"

execute_command "$MVN" clean deploy -DaltDeploymentRepository="${REPO_URL}" "$SKIP_TESTS_D_PARAM" $ADDITIONAL_MAVEN_PARAMS "$@"

t=$(date '+%H%M%S')
set -x
if [ $IS_RELEASE -eq 0 ] ; then
  find "./.dist/local-repo/org/apache/spark/" -name "*.jar" -print0 | while read -d $'\0' file; do
    new_name=$(echo "$file" | sed -E  "s/-([[:digit:]]+)\.[[:digit:]]+-([[:digit:]]+)/-SNAPSHOT/")
    mv "$file" "$new_name"
  done
  find "./.dist/local-repo/org/apache/spark/" -name "*.pom" -print0 | while read -d $'\0' file; do
    new_name=$(echo "$file" | sed -E  "s/-([[:digit:]]+)\.[[:digit:]]+-([[:digit:]]+)/-SNAPSHOT/")
    mv "$file" "$new_name"
  done
fi

echo -e "Build Successful"
