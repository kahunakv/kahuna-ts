# Development

```bash
npm run typecheck
npm run build
npm test
```

## Test cluster

Most tests run against a live Kahuna cluster. Start the three-node Docker cluster
before you run them:

```bash
docker compose -f kahuna/docker/local.yml up -d
npm test
docker compose -f kahuna/docker/local.yml down
```

The tests expect `https://localhost:8082`, `https://localhost:8084` and
`https://localhost:8086`. To use another cluster, set `KAHUNA_TEST_ENDPOINTS` to a
comma-separated list. An example is the cluster that `scripts/run-cluster.sh`
starts without Docker:

```bash
KAHUNA_TEST_ENDPOINTS=https://localhost:8082 npm test
```

## Optional tests

| Test | Runs when |
| --- | --- |
| Two watch tests for progress watermarks | `KAHUNA_TEST_COMMIT_ADMISSION=1`, and the nodes run with `--commit-admission-lag-ms`. |
| Origin label test | Expects the label when `KAHUNA_TEST_ORIGIN_TAGS=1` and the nodes run with `--transaction-origin-tags`. Otherwise it expects no label. |

## Tests without a cluster

- `test/value-parsing.test.ts`
- `test/placement-hash.test.ts`

The second test checks the placement hash against values from the server code. The
placement hash decides which node owns a key. If the client computes it
differently, it sends some keys to the wrong node.
