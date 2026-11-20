# Incremental resyncs: add integration test coverage before wider rollout

Incremental resyncs are being enabled for more integrations over the next few weeks. Instead of re-reading everything on each run, an integration fetches only what changed since its last successful run, and Ocean keeps a cursor in Port to remember how far each run got.

Most of the test coverage for this was lost when the cursor handling was reworked; what's left only covers the DSP lifecycle notifications and metrics. Cursor bugs don't fail loudly. They either re-ingest too much or silently skip changes, so we want coverage in place before more integrations depend on this.

Please add integration tests covering:

1. **Cursor progression across runs**: the first run, when nothing has been stored yet; saving progress after a successful run; and resuming from it on the next run. Handlers read the cursor while an incremental run is in progress.
2. **Failed kinds**: a kind that fails must not advance its cursor, and the run stops there.
3. **Multiple mapping blocks**: the same kind can be mapped more than once, and incremental kinds can sit alongside kinds that only support full resyncs. Each mapping block keeps its own progress.
4. **Storing cursors in Port**: reading, creating and updating cursors through Port's API.
5. **Fetching only what changed**: the helpers integrations use to turn the cursor into API requests, meaning server-side timestamp filters, and client-side cut-off for APIs that return results newest-first.

Notes:
- These should be integration-level tests that exercise incremental runs end to end through the resync pipeline. Port's API can be faked.
- Follow the existing test layout and reuse the shared test fixtures where possible.
- No production code changes are expected. If you find what looks like a bug, flag it rather than fixing it.
