# Configuration

Pass the options to the `KahunaClient` constructor.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `endpoints` | — | One node URL, or several. Required. |
| `transport` | `'grpc'` | `'grpc'`, `'rest'`, or your own `Transport`. See [Transports](#transports). |
| `routing` | `'auto'` | `'auto'`, `'roundRobin'`, `'learned'` or `'metadata'`. See [Routing](#routing). |
| `routingEndpointMap` | — | Maps advertised endpoints onto the URLs this client dials. |
| `allowUnlistedRoutingEndpoints` | `false` | Lets the client dial an endpoint no option named. |
| `routeCacheCapacity` | `4096` | How many learned routes the client holds. |
| `routeHintLifetimeMs` | `60_000` | How long a learned route is used before it is observed again. |
| `routingEndpointCooldownMs` | `5_000` | How long a failed endpoint is held out of routing. |
| `upgradeUrls` | `false` | Keeps a lock handle on the node that served its acquisition. |
| `defaultOperationTimeoutMs` | `30_000` | Deadline for a call whose caller passed no `AbortSignal`. See [Cancellation](errors.md#cancellation). |
| `allowInsecureCertificateValidation` | `false` | Development only. See [TLS](#tls). |
| `trustedServerCertificateThumbprints` | `[]` | SHA-256 pins, in hexadecimal. See [TLS](#tls). |
| `useHttp2` | `false` | REST transport only. |
| `bearerToken` | `'xxx'` | REST transport only. |
| `adminApiKey` | — | Sent with the admin calls only. See [Admin key](cluster.md#admin-key). |

All durations are in milliseconds.

## Transports

```ts
new KahunaClient({ endpoints, transport: 'grpc' }); // the default
new KahunaClient({ endpoints, transport: 'rest' });
```

Both transports implement the same operations and give the same results.

- gRPC has less overhead per operation.
- REST needs no HTTP/2 and is easier to put behind a proxy.

The `.proto` files ship inside the package and load at run time. The gRPC
transport needs no `protoc` and no code generation step.

## Routing

A Kahuna cluster divides its keys into partitions. Each partition lives on one
node. Any node accepts any request and forwards it to the node that owns the key.

Routing lets the client send a request straight to the owner node, so the cluster
does not forward it:

| Mode | Behavior |
| --- | --- |
| `'auto'` | `'learned'` when you give several endpoints. `'roundRobin'` when you give one. The default. |
| `'roundRobin'` | Rotates through the endpoints. No routing cache. |
| `'learned'` | Learns the owner of a key from a hint in each response. |
| `'metadata'` | Reads the routing map of the cluster in the background, so it knows the owner of a key it never used. |

Routing changes speed only, never the result of an operation. A stale route costs
one forward between nodes.

### Endpoint map

A node advertises the address that its peers use. Your client can dial a different
address, for example through a tunnel or a container port. Map the two addresses:

```ts
new KahunaClient({
  endpoints: ['https://localhost:8082'],
  routingEndpointMap: { 'https://10.0.0.2:8082': 'https://localhost:8082' },
});
```

The client refuses a hint that the map does not cover. It stays on endpoint
rotation and does not dial an address that you did not choose.

## TLS

`allowInsecureCertificateValidation` turns off certificate validation. Use it for
local development only. Without validation, an attacker on the network can read
and change the traffic.

For a self-signed certificate in production, pin its SHA-256 digest instead:

```ts
new KahunaClient({
  endpoints,
  trustedServerCertificateThumbprints: ['3A7BD3E2360A3D29EEA436FCFB7E44C735D117C4'],
});
```
