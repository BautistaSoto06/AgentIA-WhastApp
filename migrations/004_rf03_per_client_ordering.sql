-- RF-03: Per-client message ordering
--
-- No schema changes required. Ordering is enforced at the application layer:
-- - Redis locking per sender (burst:lock:<sender>)
-- - Bull queue re-queuing with exponential backoff when lock unavailable
-- - Atomic TAKE_SCRIPT ensures only one burst per sender processes at a time
--
-- This migration serves as a marker for RF-03 implementation.
-- Key changes:
-- 1. burstAggregator: Added LOCK_PREFIX, LOCK_TTL_MS, RELEASE_LOCK_SCRIPT
-- 2. BurstJobData: Added lockToken field
-- 3. burstWorker: Increased concurrency to 20, added lock contention handling
-- 4. processBurst: Implements locking and lock release in finally block
-- 5. Tests: rf03-per-client-ordering.integration.test.ts validates ordering

SELECT 1;
