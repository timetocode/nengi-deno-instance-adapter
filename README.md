# nengi-deno-instance-adapter

Native Deno server adapter for nengi using `Deno.serve`,
`Deno.upgradeWebSocket`, and the `nengi-dataviews` binary backend.

Keep the complete Nengi package family on one exact version:

```sh
deno add npm:nengi@2.0.0-rc.126 \
    npm:nengi-deno-instance-adapter@2.0.0-rc.126 \
    npm:nengi-dataviews@2.0.0-rc.126
```

```ts
import { Context, Instance } from 'npm:nengi@2.0.0-rc.126'
import { DenoInstanceAdapter } from 'npm:nengi-deno-instance-adapter@2.0.0-rc.126'

const context = new Context()
const instance = new Instance(context)
const adapter = new DenoInstanceAdapter(instance.network)

adapter.listen({ port: 8079, hostname: '0.0.0.0' })
```

To share an existing `Deno.serve` application, route requests through
`adapter.handle(request, info)`. `adapter.upgrade(request, info)` is available
when routing has already verified that the request is a WebSocket upgrade.

The adapter bounds queued outbound data with `maxBufferedBytes` (4 MiB by
default). Crossing that limit closes the socket and throws from `send`, so
nengi performs its normal immediate user and channel cleanup. Deno does not
offer hard termination for server WebSockets; nengi still removes timed-out
users synchronously, while the transport's longer idle timeout remains a
fallback for the underlying socket.

Import only from package roots. See the
[nengi manual](https://github.com/timetocode/nengi/tree/rc/2.0.0/docs/ai) for
connection lifecycle, timing, and deployment guidance.
