# NetSuite Split Migration Guide

## Context
- The `netsuite-darpan` component now owns the NetSuite fetching and two-CSV reconciliation logic that previously lived entirely inside `darpan`. The public `reconciliation.ReconciliationInventoryServices.*` contracts remain as lightweight wrappers, so existing callers continue to work without code changes.
- This guide documents the artifact map, the recommended cutover sequence, and the rollback checklist that align with the `NetSuiteInventoryServices` implementation described in the [bulk wrapper contract](bulk-wrapper-contract.md).

## Artifact ownership map
| Asset | Current owner | Notes |
| --- | --- | --- |
| `ReconciliationInventoryServices.fetch#NsInventoryAdjustments` | `darpan` (wrapper)<br>`netsuite-darpan` (implementation) | Wrapper: `darpan/src/main/groovy/darpan/reconciliation/inventory/fetchNsInventoryAdjustments.groovy` proxies to `netsuite-darpan/src/main/groovy/netsuite/reconciliation/inventory/fetchNsInventoryAdjustments.groovy`. |
| `ReconciliationInventoryServices.fetch#NsInventoryAdjustmentsBulk` | `darpan` (wrapper) / `netsuite-darpan` (bulk logic) | Wrapper: `darpan/src/main/groovy/darpan/reconciliation/inventory/fetchNsInventoryAdjustmentsBulk.groovy` forwards to `NetSuiteInventoryServices.fetch#NsInventoryAdjustmentsBulk` defined in `component://netsuite-darpan/service/reconciliation/NetSuiteInventoryServices.xml`. The implementation enforces the deterministic 100-row chunking described in the bulk contract doc. |
| `ReconciliationInventoryServices.retrieve#InventoryAdjustmentsByReference` | `darpan` (wrapper) / `netsuite-darpan` (orchestration) | Wrapper proxies to `NetSuiteInventoryServices.retrieve#InventoryAdjustmentsByReference`, which reads HC CSV/JSON files, performs chunked NetSuite enrichment, and writes `nsChunkResults`, `summary`, and `itemResults`. |

## Cutover sequence
1. Deploy the release that contains the `netsuite-darpan` component, ensuring the new `service/reconciliation/NetSuiteInventoryServices.xml` and the `netsuite/reconciliation/inventory/*.groovy` scripts are on the classpath.
2. Verify that the `darpan` component still exposes `ReconciliationInventoryServices.*` entries (they are wrappers) and that the wrappers point at the new `NetSuiteInventoryServices` verbs via `ec.service.sync().name("reconciliation.NetSuiteInventoryServices…")`.
3. Update any runtime deployments to include both components; no configuration changes are required beyond the existing `NsRestletConfig`/`NsAuthConfig` records.
4. Run an integration validation pass using the checklist in [integration-validation.md](integration-validation.md), ensuring the chunked run has populated `nsChunkResults`, `reasonCode`/`reasonText`, and `_matchedRuleIds` as expected.
5. Once the validation pass shows green, flip the release pointer (or CI tag) that references the new component set, then monitor `summaryLocation` reports for stable chunk success ratios.

## Rollback checklist
- If a regression occurs before cutover completion, roll back to the previous release that still contained the legacy logic inside `darpan/src/main/groovy/darpan/reconciliation/inventory/*.groovy` and omit the `netsuite-darpan` component deployment.
- Confirm that the rollback bundle contains the older NetSuite bulk logic so the wrappers in `ReconciliationInventoryServices` execute locally again.
- Re-run smoke tests with the legacy release; verify the legacy output files (NS, read-db, summary) are still written to the expected runtime/tmp folder.
- After stability is restored, analyze the `summaryLocation` report from the failed run to identify chunk failures (look for `nsChunkFailureCount` and the `nsChunkResults` entries) before attempting a new cutover.

## HC data roadmap note
- HC reconciliation feeds currently use the two-CSV upload model documented here; the NetSuite connector that will eventually replace those CSV uploads is still scheduled for a later release. Continue to land HC detail via CSV files while the connector is gated.
- When the connector work begins, extend this guide to cover the validation of connector-delivered CSV/JSON assets and how they feed into the same chunked run gate.
