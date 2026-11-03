# Security scope (local engineering demo)

AstraLog's current wire protocol does **not** provide client authentication,
authorization, or TLS. A successful network handshake is not proof of identity.
Do not expose broker or controller ports to the internet, public Wi-Fi, or
untrusted machines. Use the provided demonstration with loopback (`127.0.0.0/8`)
listeners and test data only.

The public broker, internal broker replication and controller peer listener
configuration now **rejects wildcard and non-loopback bind addresses**. This
is a small fail-closed safety default, not an authentication implementation:
another program on the same machine may still connect to a loopback port.
Remote multi-machine deployment is deliberately unsupported until an
authenticated transport is designed and tested.

Limits on frame size, concurrent requests, quotas, and checksums help with
resource safety and accidental corruption. They are **not** substitutes for
authenticated transport or an authorization policy.

The Windows implementation writes and flushes files but does not expose a
portable directory `fsync`. A clean restart demonstration is **not** evidence
of safety against sudden host power loss on Windows. The intended Linux
durability behavior still needs its own environment-specific validation.

This is a portfolio implementation, **not a secure production service**.
Please do not send sensitive data through it. For security concerns, avoid
posting exploit details or secrets in a public issue.
