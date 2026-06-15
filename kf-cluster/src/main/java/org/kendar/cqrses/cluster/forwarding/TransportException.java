package org.kendar.cqrses.cluster.forwarding;

/**
 * kf-cluster-internal: the forwarding transport failed (connect refused, write
 * failed, connection dropped with requests pending, pool shut down). The
 * {@code ClusterCommandForwarder} maps this to its fallback/retry policy —
 * it never escapes to application code.
 */
public class TransportException extends RuntimeException {
    public TransportException(String message) {
        super(message);
    }

    public TransportException(String message, Throwable cause) {
        super(message, cause);
    }
}
