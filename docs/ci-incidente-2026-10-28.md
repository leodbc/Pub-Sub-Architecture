# Incident report: M2 Actions run history (2026-10-28)

Source: [Pull Request #1](https://github.com/leodbc/Pub-Sub-Architecture/pull/1) and GitHub Actions job logs/artifacts.

## Run management
- 14 PR-triggered runs were emitted during incremental commits; 7 were cancelled, 4 succeeded, 3 failed.
- The group `m2-experiments-${{ github.ref }}` with `cancel-in-progress: false` still **replaced queued runs** when new commits arrived. It did not protect the pending slot.
- Corrective policy: include `github.sha` in the group, and **batch modifications before pushing**. This avoids implicit displacement. One consolidated commit implements this change.

## Grounded results
- [#40723593375](https://github.com/leodbc/Pub-Sub-Architecture/actions/runs/40723593375) and [#40723795807](https://github.com/leodbc/Pub-Sub-Architecture/actions/runs/40723795807): successful smoke and E1–E4 jobs.
- [#40725573952](https://github.com/leodbc/Pub-Sub-Architecture/actions/runs/40725573952): smoke and E1–E4 successful; E5/E7 and E6/E8 failed; E9 skipped.
- Failed E5/E7 artifact: `m2-e5e7-40725573952-1`, id `12833771576`. Kafka E5 failover had 4 measured cycles, E7 had 3. Fourth Kafka recovery could not return ISR from `3,2` to all three. The full Kafka container logs show a native process **SIGSEGV** on kafka1 restart at approximately 16:35:05 UTC. No 5-run statistics are asserted for E5/E7.
- Failed E6/E8 artifact: `m2-e6e8-40725573952-1`, id `12832509338`. Five E6 observations in each system and five Kafka E8 elections were recorded. NATS E8 has only four observations: `/jsz` returned `meta_cluster` without `leader` in the fifth run. No 5-run NATS E8 summary is asserted.
- Kafka E4 multi-partition `sdtool verify`: 90 confirmed/unique, 0 losses, 0 per-partition out-of-order, but 71 globally out of order **across partitions**, as expected when reading partitions separately. This does not establish a global total order.

## Changes in next batch
- Run-level concurrency keyed by SHA to protect pending runs.
- More graceful Kafka shutdown for failure injection, explicit Docker process-state evidence if a native broker does not recover. A SIGSEGV remains a real failure and is not silently skipped.
- NATS metadata election checks sample three nodes and retry a bounded number of times, instead of failing when the first node temporarily lacks `meta_cluster.leader`.
- `sdtool verify` retries only a transient NATS 503 and preserves every attempt.
- Every fix must be validated by its **own GitHub Actions run**, not inferred from previous successes.

The test harness does not simulate or backfill measurements. Missing repetitions remain ND.

## Run #15 and upstream root cause
- [#40728675576](https://github.com/leodbc/Pub-Sub-Architecture/actions/runs/40728675576): smoke, E1–E4 and E5/E7 passed (20 live observations for E5/E7). E6/E8 failed during **Kafka E8 first recovery**; E9 skipped. Kafka controller election did advance LeaderEpoch from 1 to 2, but broker kafka2 failed to rejoin the ISR.
- Raw artifact `m2-e6e8-40728675576-1`, ID `12833749004`: `kafka_docker_logs.log` contains a native-image crash in `com.oracle.svm.core.posix.headers.Pwd.getpwuid` immediately after restarting kafka2. The stack signature matches Apache Kafka [KAFKA-20314](https://issues.apache.org/jira/browse/KAFKA-20314): a thread-unsafe libc lookup in the older GraalVM native image.
- Apache Kafka release [4.4.0](https://kafka.apache.org/community/downloads/) (2026-10-22) incorporates [apache/kafka#22059](https://github.com/apache/kafka/pull/22059), upgrading the native image build to GraalVM 25 and replacing `getpwuid()` with thread-safe `getpwuid_r()`.
- **Fix under test:** upgrade Kafka broker image `apache/kafka-native:4.1.0` → `4.4.0` and CLI image `apache/kafka:4.1.0` → `4.4.0` together, retaining the 3-node KRaft topology. E8 shutdown is made graceful (10s) to reduce unrelated abrupt-stop variation. This is a version change, so all measurements should be rerun under the new version rather than pooled with Kafka 4.1.0.
- The change is not considered successful until a fresh complete GitHub Actions run passes all experimental gates and uploads source evidence.
