import {
    Binary,
    Client,
    Context,
    defineMessageSchema,
    Instance,
    NetworkEvent,
    User
} from 'nengi'
import { createRequire } from 'node:module'
import { WebSocketClientAdapter } from 'nengi-websocket-client-adapter'

const require = createRequire(import.meta.url)
const { DenoInstanceAdapter } = require('../build/index.js') as typeof import('../src/index.ts')

function withTimeout<T>(promise: Promise<T>, label: string) {
    return Promise.race([
        promise,
        new Promise<never>((_, reject) => {
            setTimeout(() => reject(new Error(`${label} timed out.`)), 2_000)
        })
    ])
}

async function waitForQueueEvent(instance: Instance, type: NetworkEvent, label: string) {
    const deadline = performance.now() + 1_000
    while (performance.now() < deadline) {
        while (!instance.queue.isEmpty()) {
            const event = instance.queue.next()
            if (event.type === type) {
                return event
            }
        }
        await new Promise(resolve => setTimeout(resolve, 5))
    }
    throw new Error(`${label} timed out.`)
}

const context = new Context()
context.register(1, defineMessageSchema({ value: Binary.UInt8 }))
const instance = new Instance(context)
const adapter = new DenoInstanceAdapter(instance.network, { path: '/nengi' })
const client = new Client(context, WebSocketClientAdapter, 20)
client.setDisconnectHandler(() => {})
instance.onConnect = async handshake => ({ echoed: handshake.runtime })

{
    let closeCode = 0
    const boundedAdapter = new DenoInstanceAdapter(instance.network, { maxBufferedBytes: 8 })
    const socket = {
        readyState: WebSocket.OPEN,
        bufferedAmount: 4,
        close: (code: number) => { closeCode = code }
    }
    const user = new User(socket, boundedAdapter)
    try {
        boundedAdapter.send(user, new ArrayBuffer(8))
        throw new Error('Expected Deno backpressure to reject the send.')
    } catch (error) {
        if (closeCode !== 1013) {
            throw new Error('Deno backpressure did not request an overload close.')
        }
    }
}

await new Promise<void>(resolve => {
    adapter.listen({ port: 0, hostname: '127.0.0.1' }, resolve)
})

try {
    const port = adapter.server?.addr.port
    if (!port) {
        throw new Error('The native Deno server did not expose its assigned port.')
    }
    const httpResponse = await fetch(`http://127.0.0.1:${port}/nengi`)
    if (httpResponse.status !== 426) {
        throw new Error(`Expected the Deno route to require WebSocket upgrade, got ${httpResponse.status}.`)
    }

    const result = await withTimeout(
        client.connect(`ws://127.0.0.1:${port}/nengi`, {
            runtime: 'deno-native'
        }),
        'Native Deno nengi handshake'
    )
    if (!result.accepted) {
        throw new Error('Native Deno handshake was denied.')
    }

    const connected = await waitForQueueEvent(instance, NetworkEvent.UserConnected, 'Deno connection event')
    if (connected.payload?.echoed !== 'deno-native') {
        throw new Error('Native Deno connection payload was not preserved.')
    }

    instance.step()
    await new Promise(resolve => setTimeout(resolve, 10))
    const frames = client.network.drainFrames()
    if (frames.length !== 1 || frames[0].tick !== 1) {
        throw new Error(`Expected native Deno snapshot tick 1, received ${frames.length}.`)
    }

    const user = [...instance.users.values()][0]
    if (!user || user.clockSyncSamples < 1 || user.pendingPings.size !== 0) {
        throw new Error('Native Deno immediate ping/pong did not complete.')
    }

    client.addCommand({ ntype: 1, value: 73 })
    client.flush()
    const command = await waitForQueueEvent(instance, NetworkEvent.CommandSet, 'Deno command delivery')
    if (command.commands?.length !== 1 || command.commands[0].value !== 73) {
        throw new Error('Native Deno command payload was not preserved.')
    }

    client.disconnect('native smoke complete')
    await waitForQueueEvent(instance, NetworkEvent.UserDisconnected, 'Deno disconnect lifecycle')
    if (instance.users.size !== 0) {
        throw new Error('Native Deno disconnect did not remove the user.')
    }

    console.log('deno native adapter smoke ok')
} finally {
    client.disconnect('native smoke cleanup')
    await adapter.close()
}
