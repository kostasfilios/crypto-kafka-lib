#!/usr/bin/env bash
# Mutation testing for the event publisher with PIT (command line). Not part of the build or of JitPack: run it by
# hand to reproduce the mutation numbers.
#
#   JAVA_HOME=$(/usr/libexec/java_home -v 17) tools/mutation/run-pitest.sh
#   PIT_THREADS=4 PIT_HOME=/tmp/my-pit tools/mutation/run-pitest.sh
#
# Needs JDK 17, curl and python3; the first run downloads PIT from Maven Central into $PIT_HOME/lib.
# Writes $PIT_HOME/report/index.html and mutations.xml, then prints a summary (tools/mutation/triage.py) that also
# gives the score without the mutants of the null checks Kotlin generates (Intrinsics.checkNotNull*), which no
# test can observe.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PIT_HOME="${PIT_HOME:-${TMPDIR:-/tmp}/crypto-kafka-lib-pitest}"
PIT_THREADS="${PIT_THREADS:-8}"
MAVEN="https://repo1.maven.org/maven2"
JARS="org/pitest/pitest/1.15.8/pitest-1.15.8.jar
org/pitest/pitest-entry/1.15.8/pitest-entry-1.15.8.jar
org/pitest/pitest-command-line/1.15.8/pitest-command-line-1.15.8.jar
org/pitest/pitest-junit5-plugin/1.2.1/pitest-junit5-plugin-1.2.1.jar
org/junit/platform/junit-platform-launcher/1.9.3/junit-platform-launcher-1.9.3.jar
org/apache/commons/commons-text/1.10.0/commons-text-1.10.0.jar
org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar"

if [ -z "${JAVA_HOME:-}" ]; then echo "set JAVA_HOME to a JDK 17" >&2; exit 2; fi
mkdir -p "$PIT_HOME/lib"
PIT_CP=""
for jar in $JARS; do
  file="$PIT_HOME/lib/$(basename "$jar")"
  [ -f "$file" ] || curl -sSfL -o "$file" "$MAVEN/$jar"
  PIT_CP="$PIT_CP$file:"
done

cd "$ROOT"
EXTERNAL="$(./gradlew -q -I tools/mutation/test-classpath.gradle printTestClasspath | sed -n 's/^TESTCP=//p')"

# Mutate a snapshot, so a build running meanwhile cannot change the classes under test.
SNAP="$PIT_HOME/snapshot"
rm -rf "$SNAP" "$PIT_HOME/report"
mkdir -p "$SNAP"
cp -R build/classes/kotlin/main "$SNAP/main"
cp -R build/classes/kotlin/test "$SNAP/test"
mkdir -p "$SNAP/res-main" "$SNAP/res-test"
[ -d build/resources/main ] && cp -R build/resources/main/. "$SNAP/res-main/"
[ -d build/resources/test ] && cp -R build/resources/test/. "$SNAP/res-test/"

P=com.crp.system.libs.kafka.publisher
TARGET_CLASSES="$P.core.ChannelEventPublisher*,$P.core.FailureDispatcher*,$P.core.PublishLane*,$P.core.DropWhenFull*,\
$P.core.CallerRunsWhenFull*,$P.core.BlockWithTimeout*,$P.core.RateLimiter*,$P.core.InternalPortsKt*,\
$P.adapters.ExponentialRetryPolicy*,$P.adapters.KafkaRetriableClassifier*,$P.adapters.LoggingPublishFailureHandler*,\
$P.adapters.ChannelProducerConfig*,$P.adapters.KafkaRecordSender*,$P.spring.ConfiguredEventPublisherChannels*,\
$P.api.PublishFailure"
# Unit tests only: the Spring context tests are slow per mutant, the integration test needs a broker, and the
# real-producer tests wait 2 s for Kafka's max.block.ms.
TARGET_TESTS="$P.core.*,$P.adapters.*,$P.api.*,$P.spring.ConfiguredEventPublisherChannelsTest,$P.testing.*"
EXCLUDED_TESTS="$P.adapters.RealProducerMissingTopicTest,$P.adapters.AdminClientTopicInspectorTest"

"$JAVA_HOME/bin/java" -cp "$PIT_CP$SNAP/test:$SNAP/res-test:$SNAP/main:$SNAP/res-main:$EXTERNAL" \
  org.pitest.mutationtest.commandline.MutationCoverageReport \
  --reportDir "$PIT_HOME/report" \
  --targetClasses "$TARGET_CLASSES" \
  --targetTests "$TARGET_TESTS" \
  --excludedTestClasses "$EXCLUDED_TESTS" \
  --excludedClasses '*Test,*Test$*,*.testsupport.*,*$SummaryKey' \
  --excludedMethods 'hashCode,equals,component*,copy*' \
  --sourceDirs "$ROOT/src/main/kotlin" \
  --mutators STRONGER \
  --outputFormats HTML,XML \
  --threads "$PIT_THREADS" \
  --timeoutConst 3000 \
  --timestampedReports false

python3 "$ROOT/tools/mutation/triage.py" "$PIT_HOME/report/mutations.xml"
