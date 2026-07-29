/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Maps a throttle bucket to the single node that authoritatively owns its cluster-level counter, using a
 * consistent-hash ring built from cluster state. Every node computes the same ring from the same
 * {@link DiscoveryNodes}, so any coordinator can locate a bucket's owner with no election or lookup, and node
 * join/leave only remaps roughly {@code 1/N} of buckets.
 * <p>
 * Design choices that matter:
 * <ul>
 *   <li><b>Data nodes only.</b> Cluster-manager nodes are deliberately excluded — a domain may run zero or one
 *       dedicated cluster-manager, so they are not a dependable ownership pool.</li>
 *   <li><b>Version-filtered.</b> During a rolling upgrade, nodes older than {@link #MIN_OWNER_VERSION} (which do
 *       not run the shared-throttle handler) are excluded from the ring, so every bucket lands on a node that can
 *       actually enforce it rather than silently going unthrottled. This trades extra remapping during the
 *       upgrade window for real enforcement.</li>
 *   <li><b>Virtual nodes.</b> Each physical node is placed at {@link #VIRTUAL_NODES} points on the ring so buckets
 *       spread evenly and a departing node's share redistributes across the survivors rather than piling onto one
 *       neighbour.</li>
 *   <li><b>Empty ring ⇒ no owner.</b> When no eligible node exists (bootstrap, coordinating-only topologies, or a
 *       fleet with no upgraded data nodes yet), {@link #ownerFor} returns empty and the caller fails open.</li>
 * </ul>
 * Instances are immutable snapshots; {@link WorkloadGroupSharedThrottleService} rebuilds one whenever the
 * discovery-node set changes.
 * <p>
 * <b>Transient ceiling breach on rebalance.</b> When a bucket remaps to a new owner (node join/leave), the previous
 * owner still holds leases for that bucket's in-flight requests while the new owner starts counting from zero, so the
 * bucket's cluster-wide in-flight count can briefly exceed {@code shared_limit} (up to ~2x) until the old leases drain
 * or TTL out. This is inherent to stateless consistent hashing and is accepted for this experimental feature.
 */
@ExperimentalApi
public class ThrottleOwnerSelector {

    /**
     * First version whose nodes run the shared-throttle transport handler and may therefore own buckets. Nodes below
     * this are excluded from the ring so a bucket never maps to an owner that would answer the acquire RPC with an
     * unknown-action failure (which fails open, silently disabling {@code shared_limit} for that bucket).
     * <p>
     * TODO(merge): set this to the actual release that first ships these handlers before upstreaming. On this feature
     * branch it is a placeholder; if the handlers land in a build after {@code V_3_7_0}, this must be bumped to that
     * version, otherwise a rolling upgrade with older-but-same-major data nodes would map buckets to nodes lacking the
     * handler. (Tracked alongside the throttle-settings version-gate TODO.)
     */
    public static final Version MIN_OWNER_VERSION = Version.V_3_7_0;

    private static final int VIRTUAL_NODES = 128;

    // Ring position -> node id. TreeMap gives the ceiling/first lookup that consistent hashing needs.
    private final SortedMap<Integer, String> ring;
    private final Map<String, DiscoveryNode> nodesById;

    private ThrottleOwnerSelector(SortedMap<Integer, String> ring, Map<String, DiscoveryNode> nodesById) {
        this.ring = ring;
        this.nodesById = nodesById;
    }

    /**
     * Builds a ring from the current discovery nodes, including only data nodes at or above
     * {@link #MIN_OWNER_VERSION}.
     */
    public static ThrottleOwnerSelector fromDiscoveryNodes(DiscoveryNodes discoveryNodes) {
        final SortedMap<Integer, String> ring = new TreeMap<>();
        final Map<String, DiscoveryNode> nodesById = new HashMap<>();
        for (DiscoveryNode node : discoveryNodes.getDataNodes().values()) {
            if (node.getVersion().onOrAfter(MIN_OWNER_VERSION) == false) {
                continue;
            }
            nodesById.put(node.getId(), node);
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                // Hash the node id with a replica suffix to scatter its virtual points around the ring.
                ring.put(Murmur3HashFunction.hash(node.getId() + "#" + i), node.getId());
            }
        }
        return new ThrottleOwnerSelector(ring, nodesById);
    }

    /**
     * @return the node that owns the given bucket, or empty if the ring has no eligible node (fail open).
     */
    public Optional<DiscoveryNode> ownerFor(String bucketKey) {
        if (ring.isEmpty()) {
            return Optional.empty();
        }
        final int hash = Murmur3HashFunction.hash(bucketKey);
        // First ring point at or after the bucket's hash, wrapping around to the first point.
        SortedMap<Integer, String> tail = ring.tailMap(hash);
        Integer point = tail.isEmpty() ? ring.firstKey() : tail.firstKey();
        return Optional.ofNullable(nodesById.get(ring.get(point)));
    }

    /**
     * The eligible owner nodes in this snapshot as a set of full {@link DiscoveryNode}s. Used to decide whether a
     * rebuild is needed: comparing whole nodes (not just persistent ids) detects a same-id restart, where the node
     * keeps its persistent id but gets a new ephemeral id/address — otherwise the ring would keep the stale node and
     * {@code nodeConnected} would fail open for its buckets indefinitely.
     */
    Set<DiscoveryNode> eligibleNodeSet() {
        return new HashSet<>(nodesById.values());
    }

    /**
     * Computes the eligible owner set for a candidate {@link DiscoveryNodes} <em>without</em> building the full ring
     * (no virtual-node hashing). Lets a caller cheaply check whether membership actually changed before paying to
     * rebuild the ring on every cluster-state update.
     */
    static Set<DiscoveryNode> eligibleNodeSet(DiscoveryNodes discoveryNodes) {
        Set<DiscoveryNode> eligible = new HashSet<>();
        for (DiscoveryNode node : discoveryNodes.getDataNodes().values()) {
            if (node.getVersion().onOrAfter(MIN_OWNER_VERSION)) {
                eligible.add(node);
            }
        }
        return eligible;
    }

    boolean isEmpty() {
        return ring.isEmpty();
    }
}
