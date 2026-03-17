# Inventory Adjustment Retrieval (NS + Read DB)

This flow stages a reference file (typically from SFTP), reads item/location pairs, then fetches inventory adjustment details from:

- NetSuite Restlet (NS)
- Read-only database (JDBC)

## Configuration (UI)

Go to `Settings` and configure:

- `Settings -> NetSuite Auth`
  - Entity: `darpan.reconciliation.NsAuthConfig`
  - Stores reusable auth profiles (`NONE`/`BASIC`/`BEARER`/`OAUTH2_M2M_JWT`).
  - For `OAUTH2_M2M_JWT`, configure `tokenUrl`, `clientId`, `certId` (JWT `kid`), `privateKeyPem` (PKCS#8 RSA or EC), and optional `scope` (default: `restlets rest_webservices`).
- `Settings -> NetSuite Endpoints`
  - Entity: `darpan.reconciliation.NsRestletConfig`
  - Stores endpoint URL, method (`POST`/`PUT`/`GET`), headers JSON, timeouts, and `nsAuthConfigId`.
  - Multiple endpoint configs can reference the same auth config.
- `Settings -> Read DB`
  - Entity: `darpan.reconciliation.HcReadDbConfig`
  - Supports Host, Port, Database Name, Username/Password, Display Name, Additional Parameters, plus driver and reconciliation default table/column mapping.
  - Seed reader profile is loaded from `runtime/component/darpan/data/ReconciliationInventorySeedData.xml` with ID `darpan-test-seed`.
  - Seed RuleSets for testing are also loaded from the same file:
    - SQL template RuleSet: `DARPAN_TEST_SQL_TEMPLATE_RS`
    - Comparison RuleSet: `DARPAN_TEST_COMPARE_RS`
  - Config ID can be entered manually or left blank; when blank, Darpan auto-generates a safe ID from Display Name/Host/Database.
  - If `READ_DB_CONFIG` table is missing in a dev runtime, Darpan attempts to create it automatically on first save.
  - JDBC URL is generated internally from Host/Port/Database/Additional Parameters.

Secrets (`password`, `apiToken`) are encrypted fields in entities.

## Services

- `reconciliation.ReconciliationInventoryServices.fetch#NsInventoryAdjustments`
  - Calls NS Restlet for one `itemId` + `locationId` + date range.
  - Request payload format:
    ```json
    {
      "itemId": 21036,
      "locationId": 39,
      "from": "2026-02-23",
      "to": "2026-03-03"
    }
    ```
- `reconciliation.ReconciliationInventoryServices.fetch#ReadDbRecords`
  - Queries configured read-only DB for one `itemId` + `locationId` + date range.
  - Accepts `readDbConfigId` (default: `darpan-test-seed`).
  - Resolves table/column names from service input first, then `HcReadDbConfig`.
  - SQL resolution options:
    - pass `sqlStatementTemplate` directly, or
    - pass `sqlRuleSetId` to resolve template through Rule Engine.
  - Optional flags:
    - `useInventoryItemJoin=true` to expose a join-preference hint for SQL RuleSet logic.
- `reconciliation.ReconciliationInventoryServices.retrieve#InventoryAdjustmentsByReference`
  - Reads staged reference file (CSV/JSON), extracts item/location pairs, then calls both services per pair.
  - Production inputs must be provided explicitly (`referenceFileLocation`, `from`, `to`, `nsRestletConfigId`, `comparisonRuleSetId`, and one of `sqlRuleSetId` or `sqlStatementTemplate`); `readDbConfigId` defaults to `darpan-test-seed`.
  - Supports separate source-field mapping per system:
    - `nsItemIdField`, `nsLocationIdField`
    - `readDbItemIdField`, `readDbLocationIdField`
    - When not set, both systems default to shared `itemIdField` and `locationIdField`.
  - Requires `comparisonRuleSetId` and sends each item fact to Drools for comparison classification.
  - The service does not hardcode compare rules; `compareStatus` (or any custom field) must be set by your RuleSet/DRL.
  - Uses Spark aggregation for summary counters based on configurable status field/value parameters.
  - Writes three JSON outputs:
    - NS payload file
    - Read DB payload file
    - Summary file (per-item status + missing/diff counts + warnings)

## Example Orchestration Call

```xml
<service-call name="reconciliation.ReconciliationInventoryServices.retrieve#InventoryAdjustmentsByReference">
    <parameter name="referenceFileLocation" value="runtime://tmp/reconciliation/automation/input/ref-items.csv"/>
    <parameter name="referenceFileType" value="CSV"/>
    <parameter name="itemIdField" value="itemId"/>
    <parameter name="locationIdField" value="locationId"/>
    <parameter name="from" value="2026-02-23"/>
    <parameter name="to" value="2026-03-03"/>
    <parameter name="nsRestletConfigId" value="NS_INV_MAIN"/>
    <parameter name="readDbConfigId" value="darpan-test-seed"/>
    <parameter name="comparisonRuleSetId" value="DARPAN_TEST_COMPARE_RS"/>
    <parameter name="tableName" value="inventory_item_detail"/>
    <parameter name="itemIdColumn" value="product_id"/>
    <parameter name="locationIdColumn" value="facility_id"/>
    <parameter name="transactionDateColumn" value="effective_date"/>
    <parameter name="inventoryItemTable" value="inventory_item"/>
    <parameter name="sqlRuleSetId" value="DARPAN_TEST_SQL_TEMPLATE_RS"/>
</service-call>
```

## Example For Store Inventory Diff CSV

Use this mapping when the CSV has NS and OMS item IDs in different columns (for example, `netsuite_product_id` and `product_id`):

```xml
<service-call name="reconciliation.ReconciliationInventoryServices.retrieve#InventoryAdjustmentsByReference">
    <parameter name="referenceFileLocation" value="/Users/aditipatel/Downloads/Copy of Store Inventory Diff - Inventory Difference 27 Feb - Store Inventory Diff - Inventory Difference 27 Feb.csv"/>
    <parameter name="referenceFileType" value="CSV"/>
    <parameter name="nsItemIdField" value="netsuite_product_id"/>
    <parameter name="nsLocationIdField" value="facility_id"/>
    <parameter name="readDbItemIdField" value="product_id"/>
    <parameter name="readDbLocationIdField" value="facility_id"/>
    <parameter name="from" value="2026-02-23"/>
    <parameter name="to" value="2026-03-03"/>
    <parameter name="nsRestletConfigId" value="NS_INV_MAIN"/>
    <parameter name="readDbConfigId" value="darpan-test-seed"/>
    <parameter name="comparisonRuleSetId" value="DARPAN_TEST_COMPARE_RS"/>
    <parameter name="compareStatusField" value="compareStatus"/>
    <parameter name="missingInNsStatus" value="MISSING_IN_NS"/>
    <parameter name="missingInReadDbStatus" value="MISSING_IN_READ_DB"/>
    <parameter name="countMismatchStatus" value="COUNT_MISMATCH"/>
    <parameter name="matchedStatus" value="MATCHED_COUNT"/>
    <parameter name="noRecordStatus" value="NO_RECORDS"/>
    <parameter name="errorStatus" value="ERROR"/>
</service-call>
```

## RuleSet Expectations

For inventory retrieval summaries, your RuleSet should set a status field (default parameter: `compareStatus`) on each fact map.  
Fact keys available to rules include:

- `itemId`, `locationId`
- `nsItemId`, `nsLocationId`, `nsStatus`, `nsRecordCount`, `nsError`, `nsRecords`
- `readDbItemId`, `readDbLocationId`, `readDbStatus`, `readDbRecordCount`, `readDbError`, `readDbRecords`

Example rule action:

```drl
rule "Mark Missing In NS"
when
    $m : Map( this["nsRecordCount"] == 0 && this["readDbRecordCount"] > 0 )
then
    $m.put("compareStatus", "MISSING_IN_NS");
end
```

## Notes

- NS responses in the shape `{ ok, itemId, locationId, from, to, asOf, balance, transactions, page }` are handled explicitly.
  - If `ok=false`, the service fails with an error.
  - If `transactions` is present, each record is emitted with top-level context (`itemId`, `locationId`, `from`, `to`, `asOf`, `balance`, `page`) plus `transaction`.
- If NS response JSON wraps records in a list field (for example `data` or `results`), the NS fetch service auto-detects common list wrappers.
- Use `maxItems` on orchestration service for controlled dry runs.
- Use `nsDelayMs` when NS rate limiting is required.
- `comparisonRuleSetId` is required; comparison/diff decisions come from your configured rules.
- Services do not create RuleSet/Rule data; maintain rules through Rule Engine UI or seed data.
- `itemResults` contains the rule-mutated facts returned by Drools, including any custom fields added by rules.
