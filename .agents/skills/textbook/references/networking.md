# Networking and protocols, when relevant

## Decide whether the topic belongs in this textbook

Explain networking when it is needed to understand the changed code, its
correctness, a diagnosed failure, or its performance. Examples include changing
timeouts, retries, connection pooling, stream parsing, request handling,
authentication, or a network bottleneck. Network behavior may also be essential
background for a new integration even if the transport is hidden inside a client.

Do not add a networking chapter to every textbook. A dependency using HTTP or a
project containing connectors is not sufficient on its own. For an unrelated
local computation or refactoring, omit the protocol walkthrough. For a narrow
network change, explain the affected mechanism and just enough background;
do not cover this entire checklist. Match depth to the role of the protocol.

## Establish the actual stack

- Trace the code from our API through the client library to the server endpoint.
  Identify the application protocol, transport, negotiated version, encryption,
  proxies/load balancers, and connection owner where relevant. A database's wire
  protocol over TCP is not necessarily HTTP; HTTP/3 uses QUIC rather than TCP.
- Distinguish code observations, behavior verified in the library's source or
  protocol documentation, measurements, and hypotheses. Mark unknown details.
- Link the relevant call sites and client settings using the revision-aware
  source-link rules in `writing.md`. Explain terms from first principles in the
  user's chosen document language; retain actual API and protocol names.

## TCP mechanisms to explain as needed

- Establishment and closure: handshake, connection states, FIN versus RST,
  orderly EOF, half-close, half-open connections, and reconnect behavior.
- TCP as an ordered byte stream without application-message boundaries;
  framing by lengths/delimiters or a higher-level protocol. Partial reads/writes,
  buffering, and why one application write need not match one read or packet.
- Send/receive buffers, flow control versus congestion control, retransmission,
  loss, RTT, and head-of-line blocking. Relate them to observed stalls or latency.
- Nagle, delayed acknowledgements, `TCP_NODELAY`, batching, and their tradeoffs
  only when they affect the code or measurements; avoid universal tuning advice.
- Distinguish local write completion, TCP acknowledgement, application response,
  and database commit/durability acknowledgement. State which one the API exposes.

## HTTP mechanisms to explain as needed

- Request/response structure, methods, status codes, headers, body, and the
  endpoint contract. Separate transport failures from application error responses.
- Body framing and completion: content length, HTTP/1.1 chunked transfer,
  streaming and incremental reads, premature EOF, cancellation, and backpressure.
  Use the framing rules of the actual HTTP version.
- Connection reuse, HTTP keep-alive, pool acquisition, idle/stale connections,
  and response-body consumption/closure needed to return connections to a pool.
- HTTP/1.1 connection concurrency versus HTTP/2 streams and multiplexing;
  stream/connection flow control and TCP head-of-line blocking where relevant.
  Discuss HTTP/3/QUIC only if used or needed for a concrete comparison.
- Method semantics and endpoint-specific idempotency, redirects, pagination,
  rate limiting, `Retry-After`, and streaming responses where applicable.
  Do not infer that a request is safe to retry merely from its method or status.

## DNS, TLS, and connection lifecycle

- DNS lookup, caching/TTL, address changes, connection reuse, and service failover
  when they influence startup, reconnects, correctness, or measured latency.
- TLS (Transport Layer Security): handshake, certificate chain and hostname
  verification, trust roots, SNI, ALPN, and session resumption as applicable.
  Distinguish TLS from the unrelated abbreviation for thread-local storage.
- The cost and lifetime of each phase: pool wait, name resolution, connect,
  TLS handshake, request write, server processing, first byte, body transfer.
  Explain connect/read/write/idle/overall deadlines according to the actual
  client's semantics, rather than assuming all timeout options mean the same thing.
- TCP keepalive probes, application heartbeats, HTTP connection reuse, proxy
  idle timeouts, and pool expiration are different mechanisms. Explain the
  ones involved in a reported disconnect or hang.

## Database or service wire protocol

- Authentication, framing, encoding/decoding, request/response correlation,
  pipelining, transactions, streaming results, cursors, and cancellation as used.
- What the server's acknowledgement guarantees: receipt, execution, transaction
  commit, persistence, or replication. Read the actual API/protocol contract.
- Connection/session state and reuse: transaction boundaries, session settings,
  pooling, concurrency restrictions, and recovery after errors or disconnects.

## Failure semantics and verification

- A timeout or broken connection may leave the operation's outcome unknown.
  Explain the failure window: the server may have acted before the response
  was lost. Show implications for retries, duplicates, ordering, and data loss.
- Identify idempotency keys, deduplication, transaction boundaries, acknowledgements,
  checkpoints, bounded retries, backoff/jitter, and retry budgets as applicable.
  Do not turn TCP delivery into an end-to-end exactly-once guarantee.
- Draw an SVG sequence diagram of the relevant exchange: client, pool/proxy if
  involved, and server; label requests, responses, waits, deadlines, and failure
  points. Pair it with source snippets and explain what is known at each point.
  Keep it focused on the behavior being discussed.
- Provide evidence where available: client traces, per-phase timings, pool and
  queue metrics, `ss`, or packet captures. Explain what the evidence can establish;
  encrypted packet captures alone do not reveal application payloads.
- Separate protocol guarantees from implementation choices and configuration.
  Explain how the observed behavior affects correctness and the latency budget.
