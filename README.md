# nengi-deno-instance-adapter

Native Deno server adapter for nengi using `Deno.serve`,
`Deno.upgradeWebSocket`, and the `nengi-dataviews` binary backend.

Keep the complete Nengi package family on one exact version:

```sh
deno add npm:nengi@2.0.0-rc.127 \
    npm:nengi-deno-instance-adapter@2.0.0-rc.127 \
    npm:nengi-dataviews@2.0.0-rc.127
```

```ts
import { Context, Instance } from 'npm:nengi@2.0.0-rc.127'
import { DenoInstanceAdapter } from 'npm:nengi-deno-instance-adapter@2.0.0-rc.127'

const context = new Context()
const instance = new Instance(context)
const adapter = new DenoInstanceAdapter(instance.network)

adapter.listen({ port: 8079, hostname: '0.0.0.0' })
```

To share an existing `Deno.serve` application, route requests through
`adapter.handle(request, info)`. `adapter.upgrade(request, info)` is available
when routing has already verified that the request is a WebSocket upgrade.
Admission is reserved before upgrade; a full instance returns HTTP 503 without
creating a socket, and a failed upgrade releases the reservation.

The adapter bounds queued outbound data with `maxBufferedBytes` (4 MiB by
default). Crossing that limit closes the socket and throws from `send`, so
nengi performs its normal immediate user and channel cleanup. Deno does not
offer hard termination for server WebSockets; nengi still removes timed-out
users synchronously, while the transport's longer idle timeout remains a
fallback for the underlying socket.

Import only from package roots. See the
[nengi manual](https://github.com/timetocode/nengi/tree/rc/2.0.0/docs/ai) for
connection lifecycle, timing, and deployment guidance.

Core connection, traffic and queue budgets apply, including a default 64 KiB
client-to-server packet cap. Deno's WebSocket API does not expose an equivalent
configurable native receive cap here: core rejects oversize packets before
decoding, after native message assembly. See
[network limits](https://github.com/timetocode/nengi/blob/rc/2.0.0/docs/ai/network-limits.md)
for configuration and migration guidance.

On tested Deno 2.9.5, immediate server-side WebSocket refusal with the native
idle timeout enabled could keep the process alive after close events and
server shutdown. This also reproduced without nengi. Pre-upgrade refusals
avoid creating that socket; verify later server-initiated close/shutdown
behavior on your deployment runtime.

WebSocket text data is rejected. Deno does not expose native Ping/Pong callbacks,
so nengi cannot charge those frames to its traffic budget. Native receive and
connection controls remain a deployment concern.
