import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // The suite talks to a live Kahuna cluster, so a wedged node must fail one
    // test instead of hanging the run.
    testTimeout: 120_000,
    hookTimeout: 60_000,
    // Lock and snapshot-floor tests share cluster-global state.
    fileParallelism: false,
    sequence: { concurrent: false },
  },
});
