/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cluster-level ({@code shared_limit}) throttle tier. Each throttle bucket has one authoritative in-flight counter
 * living on the node that owns it per a consistent-hash ring ({@link ThrottleOwnerSelector}); this service is both
 * the <em>coordinator-side</em> client that acquires/releases a shared permit for an incoming request and the
 * <em>owner-side</em> host of the {@link SharedThrottleTracker} that answers those requests for the buckets it owns.
 * <p>
 * Latency: the acquire is asynchronous so the calling (transport) thread never blocks on the round-trip — critical
 * under the very bursts throttling defends against. When the coordinator itself owns the bucket, the acquire
 * short-circuits to a direct in-memory tracker call with no network hop at all.
 * <p>
 * Availability: any failure to reach the owner (timeout, disconnect, missing handler on an old node, or an empty
 * ring) fails <em>open</em> — the request is admitted with no shared permit. This is a deliberate, un-toggleable
 * choice: a single unreachable owner must not turn a network blip into a cluster-wide rejection storm for its
 * share of buckets. A stuck permit is instead reclaimed by the owner's TTL sweep.
 */
@ExperimentalApi
public class WorkloadGroupSharedThrottleService implements ClusterStateListener {

    /** Internal transport action names (not client-facing REST). */
    public static final String ACQUIRE_ACTION_NAME = "internal:wlm/throttle/shared/acquire";
    public static final String RELEASE_ACTION_NAME = "internal:wlm/throttle/shared/release";

    // Fixed operational constants. Deliberately not cluster settings: they are internal-coordination knobs an operator
    // would never need to tune, and fail-open + a generous TTL make them safe as constants. Promote to a setting later
    // (non-breaking) only if a real deployment need emerges.
    //
    // Timeout for the acquire round-trip to a bucket's owner. Small so a slow/unreachable owner fails open quickly
    // rather than adding latency to the request path.
    static final TimeValue ACQUIRE_TIMEOUT = TimeValue.timeValueMillis(200);
    // Time-to-live for a permit on the owner. Reclaims a permit left behind by a crashed coordinator or a lost release
    // RPC. Set generously above the typical request duration so a still-running request's permit is not reclaimed early.
    // Accepted tradeoff: permits are NOT renewed, so a search that runs longer than this TTL has its permit reclaimed
    // while still executing, which can transiently admit one-or-more requests beyond shared_limit for that bucket
    // (self-correcting, fail-open direction — same flavor as the ring-rebalance transient breach). Long-running search
    // is uncommon (no default search timeout, but heavy aggs/scripts can exceed it); if it becomes a problem the fix is
    // permit renewal or deriving the TTL from a request deadline, not a larger constant.
    static final long PERMIT_TTL_NANOS = TimeValue.timeValueMinutes(5).nanos();
    // How often the owner sweeps expired permits. This is a pure memory-hygiene backstop, NOT a correctness mechanism:
    // tryAcquire() already prunes a bucket's expired permits on every acquire, so an active bucket self-heals and can
    // never over-reject due to a stuck permit. The sweep only reclaims the map entry for a bucket that abandoned a permit
    // and then went completely idle (rejects nothing). An entry can't be reclaimed before its TTL elapses anyway, so
    // sweeping faster than the TTL is pointless; run it infrequently.
    static final TimeValue SWEEP_INTERVAL = TimeValue.timeValueMinutes(5);

    private static final Logger logger = LogManager.getLogger(WorkloadGroupSharedThrottleService.class);

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final SharedThrottleTracker tracker;

    // Immutable ring snapshot, swapped wholesale on discovery-node changes so readers never see a half-built ring.
    private final AtomicReference<ThrottleOwnerSelector> ring = new AtomicReference<>();
    private volatile Scheduler.Cancellable sweepTask;

    public WorkloadGroupSharedThrottleService(ClusterService clusterService, ThreadPool threadPool, TransportService transportService) {
        this(clusterService, threadPool, transportService, new SharedThrottleTracker());
    }

    public WorkloadGroupSharedThrottleService(
        ClusterService clusterService,
        ThreadPool threadPool,
        TransportService transportService,
        SharedThrottleTracker tracker
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.tracker = tracker;
        // Start with an empty ring (fail-open) rather than reading cluster state here: during node construction the
        // ClusterApplierService has no state yet. The ring is populated by the first clusterChanged whose eligible
        // owner set differs from this empty ring — which includes a single-node cluster's very first applied state
        // (its node set does not "change" relative to the coordinator-seeded initial state, so we must NOT gate on
        // nodesChanged()).
        this.ring.set(ThrottleOwnerSelector.fromDiscoveryNodes(DiscoveryNodes.EMPTY_NODES));
        clusterService.addListener(this);
        transportService.registerRequestHandler(
            ACQUIRE_ACTION_NAME,
            ThreadPool.Names.SAME,
            AcquirePermitRequest::new,
            (request, channel, task) -> channel.sendResponse(handleAcquire(request))
        );
        transportService.registerRequestHandler(
            RELEASE_ACTION_NAME,
            ThreadPool.Names.SAME,
            ReleasePermitRequest::new,
            (request, channel, task) -> {
                tracker.release(request.bucketKey, request.permitId);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        );
    }

    /** Starts the periodic TTL sweep. Idempotent-safe to call once at node start. */
    public void start() {
        // The ring is populated by clusterChanged (see the constructor), not here: at node start the initial cluster
        // state may not be applied yet, so reading clusterService.state() here can throw "initial cluster state not
        // set yet". This method only schedules the memory-hygiene sweep.
        sweepTask = threadPool.scheduleWithFixedDelay(() -> {
            try {
                tracker.sweepExpired();
            } catch (Exception e) {
                logger.warn("Shared throttle TTL sweep failed", e);
            }
        }, SWEEP_INTERVAL, ThreadPool.Names.GENERIC);
    }

    public void stop() {
        if (sweepTask != null) {
            sweepTask.cancel();
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // Rebuild whenever the eligible-owner set differs from the current ring, rather than gating on
        // event.nodesChanged(). nodesChanged() is a delta vs the previous applied state, and the coordinator seeds the
        // initial applied state already containing the local node — so on a single-node cluster (or any cluster whose
        // membership is stable after this service starts) nodesChanged() is never true and the ring would stay empty,
        // silently disabling shared throttling.
        //
        // Compare the eligible-node set FIRST, without building the ring (cheap: a version-filtered data-node scan, no
        // virtual-node hashing), so an unchanged membership — the common case, including every cluster-state update
        // while WLM is disabled — pays nothing. Compare whole DiscoveryNodes, not just persistent ids, so a same-id
        // restart (new ephemeral id/address) is treated as a change; otherwise the ring would keep the stale node and
        // its buckets would fail open forever.
        Set<DiscoveryNode> candidate = ThrottleOwnerSelector.eligibleNodeSet(event.state().nodes());
        if (ring.get().eligibleNodeSet().equals(candidate) == false) {
            ring.set(ThrottleOwnerSelector.fromDiscoveryNodes(event.state().nodes()));
        }
    }

    /**
     * Asynchronously acquires one cluster-level shared permit for {@code bucketKey}, notifying {@code listener} with:
     * <ul>
     *   <li>a non-null {@link Releasable} — granted; close it on request completion to release the shared permit;</li>
     *   <li>{@code null} — <em>fail open</em>: admit the request without a shared permit (owner unreachable, empty
     *       ring, or any error). Nothing to release.</li>
     * </ul>
     * A denial (bucket at its shared limit) is signalled via {@link ActionListener#onFailure} with a
     * <em>message-less</em> {@link OpenSearchRejectedExecutionException} — a pure "denied" marker. This service holds
     * only the opaque bucket key, not the human-readable group/attribute, so {@link WorkloadGroupService} recomposes
     * the user-facing 429 message; the exception's <em>type</em> (not its text) is what distinguishes a denial from a
     * transport error (which also arrives via {@code onFailure} but must pass through as fail-open, not a 429).
     * The listener may be invoked inline (local-owner short-circuit) or on a transport thread.
     */
    public void acquireAsync(String bucketKey, int sharedLimit, ActionListener<Releasable> listener) {
        final ThrottleOwnerSelector currentRing = ring.get();
        final DiscoveryNode owner = currentRing.ownerFor(bucketKey).orElse(null);
        if (owner == null) {
            listener.onResponse(null); // empty ring -> fail open
            return;
        }

        final String permitId = permitId();
        final long ttlNanos = PERMIT_TTL_NANOS;

        // Local-owner short-circuit: this coordinator owns the bucket, so hit the tracker directly with no network hop.
        if (owner.getId().equals(clusterService.localNode().getId())) {
            if (tracker.tryAcquire(bucketKey, sharedLimit, permitId, ttlNanos)) {
                listener.onResponse(releaseLocal(bucketKey, permitId));
            } else {
                listener.onFailure(deniedMarker());
            }
            return;
        }

        // Owner known-disconnected: fail open immediately rather than sending into a dead socket and waiting out
        // ACQUIRE_TIMEOUT. This is an in-memory connection-map lookup (ConcurrentHashMap.containsKey), not a network
        // call, so it costs nothing. It collapses the bulk of the post-node-drop detection window; the tiny remaining
        // sub-window (socket dead but not yet flagged) still hits the timeout and is handled by handleException below.
        if (transportService.nodeConnected(owner) == false) {
            listener.onResponse(null);
            return;
        }

        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(ACQUIRE_TIMEOUT).build();
        transportService.sendRequest(
            owner,
            ACQUIRE_ACTION_NAME,
            new AcquirePermitRequest(bucketKey, sharedLimit, permitId, ttlNanos),
            options,
            new TransportResponseHandler<AcquirePermitResponse>() {
                @Override
                public AcquirePermitResponse read(StreamInput in) throws IOException {
                    return new AcquirePermitResponse(in);
                }

                @Override
                public void handleResponse(AcquirePermitResponse response) {
                    if (response.granted) {
                        listener.onResponse(releaseRemote(owner, bucketKey, permitId));
                    } else {
                        listener.onFailure(deniedMarker());
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    // Owner unreachable / timed out -> fail open. The owner may have GRANTED this permit before the
                    // response was lost (e.g. the acquire arrived but the reply timed out), which would otherwise
                    // occupy a shared slot until its TTL and cause false 429s once connectivity recovers. Send a
                    // best-effort release for this permitId to reclaim it immediately; release-by-id is idempotent, so
                    // if no permit was created it is a harmless no-op. Then admit (fail open).
                    logger.debug("Shared throttle acquire to owner [{}] for bucket [{}] failed; failing open", owner.getId(), bucketKey);
                    sendRelease(owner, bucketKey, permitId);
                    listener.onResponse(null);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
    }

    // Owner-side admission. Package-private for tests.
    AcquirePermitResponse handleAcquire(AcquirePermitRequest request) {
        boolean granted = tracker.tryAcquire(request.bucketKey, request.sharedLimit, request.permitId, request.ttlNanos);
        return new AcquirePermitResponse(granted);
    }

    private Releasable releaseLocal(String bucketKey, String permitId) {
        return releaseOnce(() -> tracker.release(bucketKey, permitId));
    }

    private Releasable releaseRemote(DiscoveryNode owner, String bucketKey, String permitId) {
        return releaseOnce(() -> sendRelease(owner, bucketKey, permitId));
    }

    // Fire-and-forget RELEASE RPC to the bucket owner. Bounded by the same timeout as acquire so a half-open
    // connection can't leave the response handler pending until the connection is torn down. A lost release is not
    // fatal — the owner's TTL sweep reclaims the permit — so failures are logged at debug only.
    private void sendRelease(DiscoveryNode owner, String bucketKey, String permitId) {
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(ACQUIRE_TIMEOUT).build();
        transportService.sendRequest(
            owner,
            RELEASE_ACTION_NAME,
            new ReleasePermitRequest(bucketKey, permitId),
            options,
            new TransportResponseHandler<TransportResponse.Empty>() {
                @Override
                public TransportResponse.Empty read(StreamInput in) {
                    return TransportResponse.Empty.INSTANCE;
                }

                @Override
                public void handleResponse(TransportResponse.Empty response) {}

                @Override
                public void handleException(TransportException exp) {
                    logger.debug(
                        "Shared throttle release to owner [{}] for bucket [{}] failed; TTL will reclaim",
                        owner.getId(),
                        bucketKey
                    );
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
    }

    // Guards against a double close (e.g. onRequestEnd and onRequestFailure) releasing twice.
    private static Releasable releaseOnce(Runnable release) {
        final AtomicBoolean released = new AtomicBoolean(false);
        return () -> {
            if (released.compareAndSet(false, true)) {
                release.run();
            }
        };
    }

    private static String permitId() {
        return UUIDs.base64UUID();
    }

    // Message-less "denied" marker: the bucket is at its shared limit. Carries no user text because this service has
    // only the opaque bucket key; WorkloadGroupService recomposes the user-facing 429 with the group name/attribute.
    // The exception TYPE (OpenSearchRejectedExecutionException) is the signal — the orchestrator uses it to tell a
    // denial (recompose as a 429) apart from a transport error (pass through as fail-open).
    private static OpenSearchRejectedExecutionException deniedMarker() {
        return new OpenSearchRejectedExecutionException();
    }

    // Package-private accessor for tests.
    SharedThrottleTracker tracker() {
        return tracker;
    }

    ThrottleOwnerSelector ring() {
        return ring.get();
    }

    /**
     * Shared body helpers for the RPC types below. All three serialize their body as ONE count-prefixed, name-keyed map
     * rather than as positional fields, so a peer built from a different commit of the same release stays interoperable.
     * OpenSearch transport serde is otherwise positional, and a {@code Version} gate cannot separate two builds that
     * report the same version — which is exactly what a blue/green deployment produces.
     * <p>
     * <b>The rule for reading depends on when a field was introduced, not on what the field does.</b>
     * <ul>
     *   <li><b>Baseline fields</b> — the ones below, present since this map format was introduced. Every build that speaks
     *       this protocol sends them, so absence means genuine corruption or a truly incompatible peer, never ordinary
     *       version skew. Read them with {@code require*}: a throw happens while the transport layer is decoding, before
     *       any handler runs, so it becomes a clean error response — exactly what the positional format already did for a
     *       malformed message. Strictness therefore costs nothing and preserves the existing behavior.</li>
     *   <li><b>Fields added after this point</b> — may be read strictly ONLY where the sender is guaranteed to have sent
     *       them, which in practice means gating the read on the stream's version. That version is the NEGOTIATED MINIMUM
     *       of the two nodes ({@code Version.min}, see NativeOutboundHandler), not the sender's alone — which is what
     *       makes the gate safe here: min is never above the sender, so a peer that predates the field always takes the
     *       else branch and the strict read never runs.
     *       <pre>
     *       if (in.getVersion().onOrAfter(V_X)) {{@code newField = requireString(body, KEY_NEW);}}
     *       else {{@code newField = SOME_DEFAULT;}}
     *       </pre>
     *       That is preferable to an unconditional default where it applies, because it still catches a peer that should
     *       have sent the field. It does NOT apply to the case this format exists for: two builds reporting the SAME
     *       version, one with the field and one without. A version gate cannot separate those, so a field that can appear
     *       in same-version skew must be read with a safe default — otherwise it throws on EVERY message from EVERY older
     *       build for the whole rollout window, systematically, discarding the benefit of this format.</li>
     * </ul>
     * Values are generic, so a future field keeps its natural type. Stick to types in {@code StreamOutput.WRITERS}
     * (String, Integer, Long, Boolean, List, Map): a value whose type an older peer's {@code readGenericValue} does not
     * know would throw while decoding, defeating the tolerance.
     */
    private static Map<String, Object> readBody(StreamInput in) throws IOException {
        return in.readMap(StreamInput::readString, StreamInput::readGenericValue);
    }

    // --- baseline fields: strict, matching what the positional format did for a malformed message ---

    private static String requireString(Map<String, Object> body, String key) {
        final Object value = body.get(key);
        if (value instanceof String s) {
            return s;
        }
        throw new IllegalStateException("wlm shared-throttle: key [" + key + "] missing or not a String [" + value + "]");
    }

    private static Number requireNumber(Map<String, Object> body, String key) {
        final Object value = body.get(key);
        if (value instanceof Number n) {
            return n;
        }
        throw new IllegalStateException("wlm shared-throttle: key [" + key + "] missing or not a Number [" + value + "]");
    }

    private static boolean requireBoolean(Map<String, Object> body, String key) {
        final Object value = body.get(key);
        if (value instanceof Boolean b) {
            return b;
        }
        throw new IllegalStateException("wlm shared-throttle: key [" + key + "] missing or not a Boolean [" + value + "]");
    }

    // Adding a field? Read it strictly only if a version gate guarantees the sender has it; if it can be absent from a
    // same-version build, read it with a safe default instead. See the rule above.

    /**
     * {@code coord -> owner}. Acquire RPC: a coordinator asks the bucket's owner to admit one request under
     * {@code sharedLimit}.
     */
    public static class AcquirePermitRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_SHARED_LIMIT = "shared_limit";
        static final String KEY_PERMIT_ID = "permit_id";
        static final String KEY_TTL_NANOS = "ttl_nanos";

        final String bucketKey;
        final int sharedLimit;
        final String permitId;
        final long ttlNanos;

        AcquirePermitRequest(String bucketKey, int sharedLimit, String permitId, long ttlNanos) {
            this.bucketKey = bucketKey;
            this.sharedLimit = sharedLimit;
            this.permitId = permitId;
            this.ttlNanos = ttlNanos;
        }

        AcquirePermitRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.bucketKey = requireString(body, KEY_BUCKET);
            this.sharedLimit = requireNumber(body, KEY_SHARED_LIMIT).intValue();
            this.permitId = requireString(body, KEY_PERMIT_ID);
            this.ttlNanos = requireNumber(body, KEY_TTL_NANOS).longValue();
            // Any other key is a field this build does not know about: ignored on purpose. That is the tolerance.
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(
                Map.of(KEY_BUCKET, bucketKey, KEY_SHARED_LIMIT, sharedLimit, KEY_PERMIT_ID, permitId, KEY_TTL_NANOS, ttlNanos),
                StreamOutput::writeString,
                StreamOutput::writeGenericValue
            );
        }
    }

    /**
     * {@code owner -> coord}. Acquire RPC response: whether the owner granted a shared permit.
     */
    public static class AcquirePermitResponse extends TransportResponse {
        static final String KEY_GRANTED = "granted";

        final boolean granted;

        AcquirePermitResponse(boolean granted) {
            this.granted = granted;
        }

        AcquirePermitResponse(StreamInput in) throws IOException {
            final Map<String, Object> body = readBody(in);
            this.granted = requireBoolean(body, KEY_GRANTED);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(Map.of(KEY_GRANTED, granted), StreamOutput::writeString, StreamOutput::writeGenericValue);
        }
    }

    /**
     * {@code coord -> owner}. Release RPC: fire-and-forget, tells the owner to drop a previously-granted permit.
     */
    public static class ReleasePermitRequest extends TransportRequest {
        static final String KEY_BUCKET = "bucket_key";
        static final String KEY_PERMIT_ID = "permit_id";

        final String bucketKey;
        final String permitId;

        ReleasePermitRequest(String bucketKey, String permitId) {
            this.bucketKey = bucketKey;
            this.permitId = permitId;
        }

        ReleasePermitRequest(StreamInput in) throws IOException {
            super(in);
            final Map<String, Object> body = readBody(in);
            this.bucketKey = requireString(body, KEY_BUCKET);
            this.permitId = requireString(body, KEY_PERMIT_ID);
            // Any other key is a field this build does not know about: ignored on purpose. That is the tolerance.
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(
                Map.of(KEY_BUCKET, bucketKey, KEY_PERMIT_ID, permitId),
                StreamOutput::writeString,
                StreamOutput::writeGenericValue
            );
        }
    }
}
