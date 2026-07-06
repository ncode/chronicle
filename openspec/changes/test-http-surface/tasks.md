# Tasks — test the HTTP surface through its interface

## 1. Sequencing

- [x] 1.1 Confirm `juliano/fix-review-findings` is merged; rebase this change on the
  post-merge `main` (this change extends `query_service_test.go` and
  `ingest_handler_test.go`, which land there)
  - Implemented stacked on `juliano/fix-review-findings` instead of waiting for it to merge.

## 2. Read/admin surface — no-Postgres wiring tests (internal/query)

- [x] 2.1 Role/param matrix for `/v1/admin/reset-watermark` via `Handler().ServeHTTP` +
  recorder: unauthenticated→401, reader→403, admin with missing certname→400
- [x] 2.2 `handleQuery` param mapping: missing `q`→400, unparsable DSL→400, `q` longer
  than `maxDSLLen`→413
- [x] 2.3 `handleDiff` param mapping: non-RFC3339 `from`/`to`→400; state endpoint
  non-RFC3339 `at`→400

## 3. Read/admin surface — Postgres-backed behavior tests (internal/query)

- [x] 3.1 Reset-watermark end-to-end: seed a node, poison the watermark with a
  future-in-bounds push, verify a normal push rejects stale, admin POST reset→200,
  verify the next push is accepted (spec: "Admin resets a watermark")
- [x] 3.2 Volatile path with `at <past T>`→422 with the typed no-history error in the
  body (spec: "Volatile path at a past T is a 422")
- [x] 3.3 Diff of an unknown certname→404 (spec: "Unknown node is a 404")

## 4. Ingest surface (internal/ingest)

- [x] 4.1 Add `Retry-After` header assertions to the existing 429 rate-limit handler test
- [x] 4.2 Add `Retry-After` header assertion to the existing 503 backpressure test

## 5. Verify

- [x] 5.1 `make test` green without `CHRONICLE_TEST_DB` (group 2 and 4 run;
  Postgres-backed tests self-skip)
- [x] 5.2 `make test-db` green (full suite, `-p 1`)
- [x] 5.3 Confirm no production file changed (or document any bug a new test exposed and
  its fix)
