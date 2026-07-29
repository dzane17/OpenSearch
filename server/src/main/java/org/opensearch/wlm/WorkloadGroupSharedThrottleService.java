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
 * share of buckets. A stuck lease is instead reclaimed by the owner's TTL sweep.
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
    // Time-to-live for a lease on the owner. Reclaims a lease left behind by a crashed coordinator or a lost release
    // RPC. Set generously above the typical request duration so a still-running request's lease is not reclaimed early.
    // Accepted tradeoff: leases are NOT renewed, so a search that runs longer than this TTL has its lease reclaimed
    // while still executing, which can transiently admit one-or-more requests beyond shared_limit for that bucket
    // (self-correcting, fail-open direction — same flavor as the ring-rebalance transient breach). Long-running search
    // is uncommon (no default search timeout, but heavy aggs/scripts can exceed it); if it becomes a problem the fix is
    // lease renewal or deriving the TTL from a request deadline, not a larger constant.
    static final long LEASE_TTL_NANOS = TimeValue.timeValueMinutes(5).nanos();
    // How often the owner sweeps expired leases. This is a pure memory-hygiene backstop, NOT a correctness mechanism:
    // tryAcquire() already prunes a bucket's expired leases on every acquire, so an active bucket self-heals and can
    // never over-reject due to a stuck lease. The sweep only reclaims the map entry for a bucket that abandoned a lease
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
            AcquireRequest::new,
            (request, channel, task) -> channel.sendResponse(handleAcquire(request))
        );
        transportService.registerRequestHandler(
            RELEASE_ACTION_NAME,
            ThreadPool.Names.SAME,
            ReleaseRequest::new,
            (request, channel, task) -> {
                tracker.release(request.bucketKey, request.leaseId);
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

        final String leaseId = leaseId();
        final long ttlNanos = LEASE_TTL_NANOS;

        // Local-owner short-circuit: this coordinator owns the bucket, so hit the tracker directly with no network hop.
        if (owner.getId().equals(clusterService.localNode().getId())) {
            if (tracker.tryAcquire(bucketKey, sharedLimit, leaseId, ttlNanos)) {
                listener.onResponse(releaseLocal(bucketKey, leaseId));
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
            new AcquireRequest(bucketKey, sharedLimit, leaseId, ttlNanos),
            options,
            new TransportResponseHandler<AcquireResponse>() {
                @Override
                public AcquireResponse read(StreamInput in) throws IOException {
                    return new AcquireResponse(in);
                }

                @Override
                public void handleResponse(AcquireResponse response) {
                    if (response.granted) {
                        listener.onResponse(releaseRemote(owner, bucketKey, leaseId));
                    } else {
                        listener.onFailure(deniedMarker());
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    // Owner unreachable / timed out -> fail open. The owner may have GRANTED this lease before the
                    // response was lost (e.g. the acquire arrived but the reply timed out), which would otherwise
                    // occupy a shared slot until its TTL and cause false 429s once connectivity recovers. Send a
                    // best-effort release for this leaseId to reclaim it immediately; release-by-id is idempotent, so
                    // if no lease was created it is a harmless no-op. Then admit (fail open).
                    logger.debug("Shared throttle acquire to owner [{}] for bucket [{}] failed; failing open", owner.getId(), bucketKey);
                    sendRelease(owner, bucketKey, leaseId);
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
    AcquireResponse handleAcquire(AcquireRequest request) {
        boolean granted = tracker.tryAcquire(request.bucketKey, request.sharedLimit, request.leaseId, request.ttlNanos);
        return new AcquireResponse(granted);
    }

    private Releasable releaseLocal(String bucketKey, String leaseId) {
        return releaseOnce(() -> tracker.release(bucketKey, leaseId));
    }

    private Releasable releaseRemote(DiscoveryNode owner, String bucketKey, String leaseId) {
        return releaseOnce(() -> sendRelease(owner, bucketKey, leaseId));
    }

    // Fire-and-forget RELEASE RPC to the bucket owner. Bounded by the same timeout as acquire so a half-open
    // connection can't leave the response handler pending until the connection is torn down. A lost release is not
    // fatal — the owner's TTL sweep reclaims the lease — so failures are logged at debug only.
    private void sendRelease(DiscoveryNode owner, String bucketKey, String leaseId) {
        final TransportRequestOptions options = TransportRequestOptions.builder().withTimeout(ACQUIRE_TIMEOUT).build();
        transportService.sendRequest(
            owner,
            RELEASE_ACTION_NAME,
            new ReleaseRequest(bucketKey, leaseId),
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

    private static String leaseId() {
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
     * Acquire RPC: a coordinator asks the bucket's owner to admit one request under {@code sharedLimit}.
     */
    public static class AcquireRequest extends TransportRequest {
        final String bucketKey;
        final int sharedLimit;
        final String leaseId;
        final long ttlNanos;

        AcquireRequest(String bucketKey, int sharedLimit, String leaseId, long ttlNanos) {
            this.bucketKey = bucketKey;
            this.sharedLimit = sharedLimit;
            this.leaseId = leaseId;
            this.ttlNanos = ttlNanos;
        }

        AcquireRequest(StreamInput in) throws IOException {
            super(in);
            this.bucketKey = in.readString();
            this.sharedLimit = in.readVInt();
            this.leaseId = in.readString();
            this.ttlNanos = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(bucketKey);
            out.writeVInt(sharedLimit);
            out.writeString(leaseId);
            out.writeVLong(ttlNanos);
        }
    }

    /**
     * Acquire RPC response: whether the owner granted a shared permit.
     */
    public static class AcquireResponse extends TransportResponse {
        final boolean granted;

        AcquireResponse(boolean granted) {
            this.granted = granted;
        }

        AcquireResponse(StreamInput in) throws IOException {
            this.granted = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(granted);
        }
    }

    /**
     * Release RPC: fire-and-forget, tells the owner to drop a previously-granted lease.
     */
    public static class ReleaseRequest extends TransportRequest {
        final String bucketKey;
        final String leaseId;

        ReleaseRequest(String bucketKey, String leaseId) {
            this.bucketKey = bucketKey;
            this.leaseId = leaseId;
        }

        ReleaseRequest(StreamInput in) throws IOException {
            super(in);
            this.bucketKey = in.readString();
            this.leaseId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(bucketKey);
            out.writeString(leaseId);
        }
    }
}
