# Message Processing Architecture

## Overview

The system processes WhatsApp messages with the following guarantees:
- **Burst Aggregation (RF-02)**: Messages from a client arriving in quick succession are batched and processed once.
- **Per-Client Ordering (RF-03)**: Messages from the same client are processed strictly in order, one burst at a time.
- **Parallel Processing**: Different clients are processed concurrently up to the worker concurrency limit.
- **Idempotence**: Message redeliveries are deduplicated and not reprocessed.

## Components

### 1. Webhook Handler (src/app.ts, src/webhooks/)

- Receives WhatsApp webhook events
- Validates signatures using HMAC-SHA256
- Performs idempotent message storage
- Returns 200 immediately to WhatsApp
- Adds messages to burst aggregation

### 2. Redis-based Burst Buffering

Per sender, maintained in Redis:
- `burst:<sender>`: FIFO list of buffered message JSON
- `burst:seq:<sender>`: Monotonic sequence counter (never reset)
- `burst:time:<sender>`: Timestamp of the burst's first message
- `burst:lock:<sender>`: Lock token while processing (RF-03)

### 3. Bull Queue (src/services/burstAggregator.ts)

**Job Type**: `BurstJobData`
- `sender`: Phone number
- `seq`: Sequence number within this sender's burst
- `firstMessageTime`: Start of current burst (ms)
- `lockToken`: Unique token for this job's lock (RF-03)

**Delay Strategy**:
- Initial delay: `min(windowMs, firstMessageTime + maxWaitMs - now)`
- Allows first message to wait up to `maxWaitMs`
- Or waits for `windowMs` silence after last message

### 4. Burst Worker (src/queue/burstWorker.ts)

**Concurrency**: 20 jobs in parallel (RF-03 allows higher global concurrency because ordering is per-sender)

**Processing Flow**:
1. Try to acquire per-sender lock (TAKE_SCRIPT)
2. If lock available: take all buffered messages for this sender
3. If lock held: return null and re-queue job with exponential backoff
4. Process burst (update DB, simulate AI work)
5. Release lock in finally block

**Lock Mechanism**:
- Lock key: `burst:lock:<sender>`
- Lock TTL: 2 minutes (failsafe for stuck jobs)
- Lock token: unique identifier per job, prevents releasing other's locks
- On error: lock still released (ensures no deadlock)

### 5. Database Schema

#### message_bursts
- `id`: Unique burst identifier
- `sender`: Phone number
- `combined_text`: Aggregated text from all messages
- `message_ids`: Array of message IDs in order
- `status`: pending → processing → completed/failed
- `created_at`: When burst started
- `processed_at`: When processing finished

#### messages
- `id`: Unique auto-increment
- `message_id`: WhatsApp message ID (unique)
- `sender`: Phone number
- `payload`: Full message content (JSON)
- `status`: pending → processing → completed/failed
- `burst_id`: Foreign key to message_bursts
- `webhook_received_at`: When webhook arrived

## Redis Scripts

### ADD_SCRIPT
Atomically adds a message to a sender's burst buffer:
- Increments sequence counter
- Sets burst start time (if first message)
- Appends message to FIFO list
- Sets TTLs

**Idempotent**: Can be called multiple times safely

### TAKE_SCRIPT (RF-03)
Atomically checks out a burst for processing:
- Verifies burst hasn't already been taken
- Verifies this job is still the latest (unless cap reached)
- **Acquires lock** (RF-03): fails if another job holds sender's lock
- Returns buffered messages and clears buffer

**Returns**:
- `nil` if job superseded, newer message pending, or lock held
- Array of messages if successfully acquired

### RELEASE_LOCK_SCRIPT (RF-03)
Releases the per-sender lock:
- Only releases if token matches (prevents cross-contamination)
- Idempotent: releasing twice is safe

## Request Flow

### Happy Path (Single Message)
1. Webhook arrives with `message_id`
2. Idempotency check: if first time, proceed; else return 200
3. Store message in DB with status=pending
4. Call ADD_SCRIPT in Redis
5. Schedule Bull job with calculated delay
6. Return 200 to WhatsApp immediately
7. (Async) Worker takes burst when job fires
8. (Async) Worker processes: AI work, update status to completed

### Concurrent Clients Example
```
Client A:  msg1 ──────────────┐
                               │ Burst A taken (lock acquired)
Client B:  msg2 ──────┐        │ Processing Burst A (delay/lock)
                      │ Burst B taken
                      │ (different sender, no lock contention)
                      Processing Burst B (parallel)
                      
Burst B completes first (no lock contention)
Burst A completes after (was locked)

Client A:  msg3 ──────┐
                      │ Burst A2 queued (waits: B1 still processing)
                      
Burst A2 takes lock, processes
```

## Configuration

```typescript
config.burst.windowMs    = 10000  // Wait for quiet after last message (ms)
config.burst.maxWaitMs   = 30000  // Max wait for typing sender (ms)
config.database.poolMin  = 2      // Min DB connections
config.database.poolMax  = 10     // Max DB connections
```

Concurrency is set in burstWorker.ts to 20 jobs in parallel.

## Guarantees

### Ordering per Client (RF-03)
- Lock mechanism ensures only one burst per sender processes at a time
- Jobs that can't acquire lock are re-queued with exponential backoff
- No job starves: Bull assigns jobs fairly

### No Duplicates
- Message ID uniqueness enforced in DB
- Idempotency table prevents redeliveries
- Burst TAKE_SCRIPT checks seq matches current burst

### No Message Loss
- All messages stored in DB before processing
- Status tracking: pending → processing → completed/failed
- Burst tracks all message IDs

### Parallel Processing
- Different senders have independent locks
- Global concurrency up to 20 jobs
- With 20 concurrent workers, up to 20 different clients can process simultaneously

## Testing

### RF-02 Tests (`tests/rf02-burst.integration.test.ts`)
- Burst aggregation with delays
- Message interleaving between senders
- Max wait cap
- Media handling

### RF-03 Tests (`tests/rf03-per-client-ordering.integration.test.ts`)
- Same client's bursts process in order
- Different clients process in parallel
- No message loss under concurrent load
- Lock cleanup on completion
- Idempotent redelivery during processing
- Lock release allows next burst

## Future Improvements

1. **Metrics**: Track lock contention, processing latency per sender
2. **Sharding**: Split senders across multiple workers for scale
3. **Webhook Response Queue**: Send responses in order once processed
4. **Adaptive Batching**: Adjust windowMs based on sender typing patterns
5. **Circuit Breaker**: Fail gracefully if AI service is slow
