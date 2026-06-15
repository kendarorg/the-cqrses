package org.kendar.cqrses.exceptions;

/**
 * A forwarded command's remote handler threw and the original exception could
 * not be reconstructed on the sender (unknown class, no {@code (String)}
 * constructor, not a {@code RuntimeException}). Carries enough to diagnose:
 * the remote exception's class name and the node that executed the command.
 * <p>
 * When reconstruction succeeds the forwarder rethrows the original type instead
 * of this wrapper, preserving the {@code sendSync} "exceptions propagate"
 * contract as transparently as possible.
 */
public class RemoteCommandException extends RuntimeException {
    private final String remoteExceptionClass;
    private final String remoteNodeId;

    public RemoteCommandException(String remoteExceptionClass, String message, String remoteNodeId) {
        super("Remote " + remoteExceptionClass + " on node " + remoteNodeId + ": " + message);
        this.remoteExceptionClass = remoteExceptionClass;
        this.remoteNodeId = remoteNodeId;
    }

    public String getRemoteExceptionClass() {
        return remoteExceptionClass;
    }

    public String getRemoteNodeId() {
        return remoteNodeId;
    }
}
