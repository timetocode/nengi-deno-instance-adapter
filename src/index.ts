import type {
    BinaryAdapter,
    BinaryPayload,
    IServerNetworkAdapter,
    ServerAdapterHost,
    ServerConnection
} from 'nengi'
import { dataViewBinary } from 'nengi-dataviews'

const DEFAULT_MAX_BUFFERED_BYTES = 4 * 1024 * 1024
const DEFAULT_IDLE_TIMEOUT_SECONDS = 120

export type DenoNetAddress = {
    transport?: string
    hostname?: string
    port?: number
}

export type DenoServeHandlerInfo = {
    remoteAddr?: DenoNetAddress
}

export interface DenoHttpServer {
    readonly addr: DenoNetAddress
    shutdown(): Promise<void>
}

export type DenoListenOptions = number | {
    port: number
    hostname?: string
    path?: string
}

export type DenoInstanceAdapterConfig = {
    binary?: BinaryAdapter<BinaryPayload, ArrayBuffer>
    path?: string
    maxBufferedBytes?: number
    idleTimeoutSeconds?: number
}

type DenoServeOptions = {
    port: number
    hostname?: string
    onListen?(address: DenoNetAddress): void
}

type DenoRuntime = {
    serve(
        options: DenoServeOptions,
        handler: (request: Request, info: DenoServeHandlerInfo) => Response | Promise<Response>
    ): DenoHttpServer
    upgradeWebSocket(request: Request, options?: { idleTimeout?: number }): {
        socket: WebSocket
        response: Response
    }
}

function getDenoRuntime(): DenoRuntime {
    const runtime = (globalThis as unknown as { Deno?: DenoRuntime }).Deno
    if (!runtime) {
        throw new Error('nengi-deno-instance-adapter requires the Deno runtime.')
    }
    return runtime
}

function closeReason(reason: unknown) {
    const serialized = typeof reason === 'string'
        ? reason
        : JSON.stringify(reason ?? 'closed')
    const value = serialized ?? 'closed'
    let clipped = value.slice(0, 123)
    while (new TextEncoder().encode(clipped).byteLength > 123) {
        clipped = clipped.slice(0, -1)
    }
    return clipped
}

function requestPath(request: Request) {
    return new URL(request.url).pathname
}

function isBinaryPayload(value: unknown): value is BinaryPayload {
    return value instanceof ArrayBuffer || ArrayBuffer.isView(value)
}

export class DenoInstanceAdapter implements IServerNetworkAdapter<BinaryPayload, ArrayBuffer, DenoListenOptions> {
    readonly network: ServerAdapterHost
    readonly binary: BinaryAdapter<BinaryPayload, ArrayBuffer>
    server: DenoHttpServer | null = null

    private readonly path: string
    private readonly maxBufferedBytes: number
    private readonly idleTimeoutSeconds: number
    private shutdownPromise?: Promise<void>

    readonly serverAdapterVersion = 1 as const

    constructor(network: ServerAdapterHost, config: DenoInstanceAdapterConfig = {}) {
        if (network?.serverAdapterVersion !== this.serverAdapterVersion) {
            throw new Error('This adapter requires nengi server adapter contract version 1. Pass instance.adapterHost from a compatible core.')
        }
        this.network = network
        this.binary = config.binary ?? dataViewBinary
        this.path = config.path ?? '/'
        this.maxBufferedBytes = config.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES
        this.idleTimeoutSeconds = config.idleTimeoutSeconds ?? DEFAULT_IDLE_TIMEOUT_SECONDS

        if (!Number.isFinite(this.maxBufferedBytes) || this.maxBufferedBytes <= 0) {
            throw new Error('DenoInstanceAdapter maxBufferedBytes must be greater than zero.')
        }
    }

    listen(options: DenoListenOptions, ready?: () => void) {
        if (this.shutdownPromise) throw new Error('DenoInstanceAdapter has shut down. Create a new adapter to listen again.')
        if (this.server) {
            throw new Error('DenoInstanceAdapter is already listening.')
        }
        const listenOptions = typeof options === 'number' ? { port: options } : options
        const path = listenOptions.path ?? this.path
        this.server = getDenoRuntime().serve({
            port: listenOptions.port,
            hostname: listenOptions.hostname,
            onListen: () => ready?.()
        }, (request, info) => this.handle(request, info, path))
    }

    handle(request: Request, info?: DenoServeHandlerInfo, path = this.path): Response {
        if (requestPath(request) !== path) {
            return new Response('Not found.', { status: 404 })
        }
        if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
            return new Response('WebSocket upgrade required.', { status: 426 })
        }
        return this.upgrade(request, info)
    }

    upgrade(request: Request, info?: DenoServeHandlerInfo): Response {
        const remoteAddress = info?.remoteAddr?.hostname ?? null
        const user = this.network.createConnection<WebSocket | null>(null, this, remoteAddress)
        user.remoteAddress = remoteAddress
        this.network.onOpen(user)
        if (user.isClosed) {
            return new Response('Connection capacity exceeded.', { status: 503 })
        }
        let upgraded: ReturnType<DenoRuntime['upgradeWebSocket']>
        try {
            upgraded = getDenoRuntime().upgradeWebSocket(request, { idleTimeout: this.idleTimeoutSeconds })
        } catch (error) {
            this.network.onClose(user, error)
            throw error
        }
        const { socket, response } = upgraded
        user.socket = socket
        let closed = false

        socket.binaryType = 'arraybuffer'
        socket.onopen = () => {
            if (user.isClosed) socket.close(1000, 'Connection closed before upgrade completed.')
        }
        socket.onmessage = event => {
            if (user.isClosed) return
            if (isBinaryPayload(event.data)) {
                this.network.onMessage(user, event.data)
                return
            }
            this.network.notifyInboundMessageError(
                user,
                new Uint8Array(),
                new Error('Nengi requires binary WebSocket messages.')
            )
            this.network.disconnectMalformedInboundUser(user)
        }
        socket.onclose = event => {
            if (closed) {
                return
            }
            closed = true
            this.network.onClose(user, event.reason)
        }
        socket.onerror = () => {
            if (closed) {
                return
            }
            closed = true
            this.network.onClose(user, 'transport_error')
        }

        return response
    }

    send(user: ServerConnection, payload: ArrayBuffer) {
        const socket = user.socket as WebSocket
        if (socket.readyState !== WebSocket.OPEN) {
            throw new Error('Cannot send a nengi snapshot on a closed Deno WebSocket.')
        }
        if (socket.bufferedAmount + payload.byteLength > this.maxBufferedBytes) {
            socket.close(1013, 'WebSocket backpressure limit exceeded.')
            throw new Error(`Deno WebSocket backpressure exceeded ${this.maxBufferedBytes} bytes.`)
        }
        socket.send(payload)
    }

    disconnect(user: ServerConnection, reason: unknown) {
        const socket = user.socket as WebSocket
        socket?.close(1000, closeReason(reason))
    }

    shutdown(reason?: any): Promise<void> {
        if (this.shutdownPromise) return this.shutdownPromise
        let finish!: () => void
        let fail!: (error: unknown) => void
        this.shutdownPromise = new Promise<void>((resolve, reject) => {
            finish = resolve
            fail = reject
        })
        const server = this.server
        this.server = null
        try {
            this.network.shutdownAdapter(this, reason)
            Promise.resolve(server?.shutdown()).then(finish, fail)
        } catch (error) {
            fail(error)
        }
        return this.shutdownPromise
    }

    /** @deprecated Use shutdown() for the common server-adapter contract. */
    close() {
        return this.shutdown()
    }
}
