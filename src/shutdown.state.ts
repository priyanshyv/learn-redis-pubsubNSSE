// Mirror of flexipill-core's src/shutdown.state.ts.
export let isShuttingDown = false;

export function markShuttingDown() {
  isShuttingDown = true;
}
