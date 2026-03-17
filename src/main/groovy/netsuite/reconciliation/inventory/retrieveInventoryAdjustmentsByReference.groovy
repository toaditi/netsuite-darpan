import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.File
import java.time.LocalDate

def logger = LoggerFactory.getLogger("netsuite.reconciliation.inventory.retrieveInventoryAdjustmentsByReference")

def normalize = { Object value -> value?.toString()?.trim() }
def normalizeBool = { Object value, boolean defaultValue ->
    if (value == null) return defaultValue
    if (value instanceof Boolean) return (boolean) value
    String raw = normalize(value)?.toLowerCase()
    if (["y", "yes", "true", "1"].contains(raw)) return true
    if (["n", "no", "false", "0"].contains(raw)) return false
    return defaultValue
}
def toInt = { Object value ->
    if (value == null) return 0
    if (value instanceof Number) return ((Number) value).intValue()
    String raw = normalize(value)
    if (!raw) return 0
    try {
        return raw.toBigDecimal().intValue()
    } catch (Exception ignored) {
        return 0
    }
}
def toDecimal = { Object value ->
    if (value == null) return BigDecimal.ZERO
    if (value instanceof BigDecimal) return (BigDecimal) value
    if (value instanceof Number) return new BigDecimal(value.toString())
    String raw = normalize(value)
    if (!raw) return BigDecimal.ZERO
    try {
        return new BigDecimal(raw)
    } catch (Exception ignored) {
        return BigDecimal.ZERO
    }
}
def warningList = [] as List<String>

def detectFileTypeFromLocation = { String location ->
    String lower = location?.toLowerCase()
    if (lower?.endsWith(".csv")) return "CSV"
    if (lower?.endsWith(".json")) return "JSON"
    return null
}

def resolvePath = { String location ->
    def rr = ec.resource.getLocationReference(location)
    if (rr != null && rr.supportsUrl()) {
        def url = rr.getUrl()
        if ("file".equalsIgnoreCase(url.protocol)) {
            try {
                return new File(url.toURI()).getAbsolutePath()
            } catch (Exception ignored) {
                return url.getPath()
            }
        }
        return url.toString()
    }
    return location
}

def writeOutputJson = { File outputDir, String prefix, String baseName, String timestamp, Map payload ->
    File out = new File(outputDir, "${prefix}-${baseName}-${timestamp}.json")
    int suffix = 1
    while (out.exists()) {
        out = new File(outputDir, "${prefix}-${baseName}-${timestamp}-${suffix}.json")
        suffix++
    }
    out.withWriter("UTF-8") { writer -> writer << JsonOutput.prettyPrint(JsonOutput.toJson(payload)) }
    return out
}

def isRetryableFailure = { String message ->
    String raw = normalize(message)?.toLowerCase()
    if (!raw) return false
    return raw.contains("http 429") || raw.contains("http 500") || raw.contains("http 502") ||
            raw.contains("http 503") || raw.contains("http 504") || raw.contains("http 408") ||
            raw.contains("timeout") || raw.contains("connection") || raw.contains("reset")
}

def parseRowJson = { String jsonText ->
    if (!jsonText) return [:]
    try {
        Object parsed = new JsonSlurper().parseText(jsonText)
        return (parsed instanceof Map) ? ((Map) parsed) : [value: parsed]
    } catch (Exception ignored) {
        return [raw: jsonText]
    }
}

def safeFieldExpr = { String expression, String label ->
    if (!(expression ==~ /[A-Za-z0-9_$.]+/)) {
        throw new IllegalArgumentException("${label} has unsupported characters: ${expression}")
    }
}

def buildRunId = {
    String seed = [normalize(referenceFileLocation), normalize(omsDetailFileLocation), normalize(from), normalize(to), normalize(nsRestletConfigId), normalize(comparisonRuleSetId)].join("|")
    String hash = Integer.toHexString(seed.hashCode())
    return "ns-run-${ec.l10n.format(ec.user.nowTimestamp, 'yyyyMMdd-HHmmss')}-${hash}"
}

def extractTransaction = { Map record ->
    if (record?.transaction instanceof Map) return (Map) record.transaction
    return record ?: [:]
}

def extractQty = { Map record ->
    Map tx = extractTransaction(record)
    for (String key in ["qty", "quantity", "adjustmentQty", "qtyDelta"]) {
        if (tx.containsKey(key)) return toDecimal(tx[key])
    }
    return BigDecimal.ZERO
}

def extractTxnDate = { Map record ->
    Map tx = extractTransaction(record)
    for (String key in ["date", "transactionDate", "trxDate", "effectiveDate", "createdAt"]) {
        String raw = normalize(tx[key])
        if (raw) return raw
    }
    return null
}

String refLocation = normalize(referenceFileLocation)
String omsDetailLocation = normalize(omsDetailFileLocation)
boolean twoCsvMode = !!omsDetailLocation

if (!twoCsvMode) {
    Map legacyInMap = [:]
    context.each { k, v -> if (!(k in ["context", "ec"])) legacyInMap[k] = v }
    Map legacyOut = ec.service.sync()
            .name("reconciliation.ReconciliationInventoryServices.retrieve#InventoryAdjustmentsByReferenceLegacy")
            .parameters(legacyInMap)
            .call()
    (legacyOut ?: [:]).each { k, v -> this."${k}" = v }
    return
}

String refTypeRaw = normalize(referenceFileType)
boolean refHasHeader = (referenceHasHeader != null) ? (referenceHasHeader as Boolean) : true
String omsTypeRaw = normalize(omsDetailFileType)
boolean omsHasHeader = (omsDetailHasHeader != null) ? (omsDetailHasHeader as Boolean) : true
String fromDateStr = normalize(from)
String toDateStr = normalize(to)
String nsConfigId = normalize(nsRestletConfigId)
String persistedNsOutputLocationValue = normalize(persistedNsOutputLocation)
boolean fetchMissingNsPairsEnabled = normalizeBool(fetchMissingNsPairs, false)
String comparisonRuleSetIdToUse = normalize(comparisonRuleSetId)
String compareStatusFieldName = normalize(compareStatusField) ?: "compareStatus"
String reasonCodeFieldName = normalize(reasonCodeField) ?: "reasonCode"
String reasonTextFieldName = normalize(reasonTextField) ?: "reasonText"
String matchedRuleIdsFieldName = normalize(matchedRuleIdsField) ?: "_matchedRuleIds"
String missingInNsStatusValue = normalize(missingInNsStatus) ?: "MISSING_IN_NS"
String missingInReadDbStatusValue = normalize(missingInReadDbStatus) ?: "MISSING_IN_READ_DB"
String countMismatchStatusValue = normalize(countMismatchStatus) ?: "COUNT_MISMATCH"
String matchedStatusValue = normalize(matchedStatus) ?: "MATCHED_COUNT"
String noRecordStatusValue = normalize(noRecordStatus) ?: "NO_RECORDS"
String errorStatusValue = normalize(errorStatus) ?: "ERROR"

int maxItemsToProcess = maxItems ? (maxItems as int) : 0
int chunkSize = nsChunkSize ? (nsChunkSize as int) : 100
if (chunkSize < 1) chunkSize = 1
if (chunkSize > 100) chunkSize = 100
int maxRetries = nsMaxRetries ? (nsMaxRetries as int) : 2
if (maxRetries < 0) maxRetries = 0
int retryBackoffMs = nsRetryBackoffMs ? (nsRetryBackoffMs as int) : 1000
if (retryBackoffMs < 0) retryBackoffMs = 0
BigDecimal backoffMultiplier = nsBackoffMultiplier ? (nsBackoffMultiplier as BigDecimal) : new BigDecimal("2.0")
if (backoffMultiplier <= BigDecimal.ONE) backoffMultiplier = new BigDecimal("2.0")
boolean continueOnChunkFailure = normalizeBool(continueOnNsChunkFailure, true)
int nsDelayMsToUse = nsDelayMs ? (nsDelayMs as int) : 0
String runIdToUse = normalize(runId) ?: buildRunId()

String defaultItemIdFieldExpr = normalize(itemIdField) ?: "itemId"
String defaultLocationIdFieldExpr = normalize(locationIdField) ?: "locationId"
String nsItemIdFieldExpr = normalize(discrepancyNsItemIdField) ?: normalize(nsItemIdField) ?: defaultItemIdFieldExpr
String nsLocationIdFieldExpr = normalize(discrepancyLocationIdField) ?: normalize(nsLocationIdField) ?: defaultLocationIdFieldExpr
String omsItemIdFieldExpr = normalize(omsItemIdField) ?: "itemId"
String omsLocationIdFieldExpr = normalize(omsLocationIdField) ?: "locationId"
String omsTxnDateFieldExpr = normalize(omsTxnDateField)
String omsQuantityFieldExpr = normalize(omsQuantityField)

if (!refLocation) throw new IllegalArgumentException("referenceFileLocation is required")
if (!omsDetailLocation) throw new IllegalArgumentException("omsDetailFileLocation is required in two-CSV mode")
if (!fromDateStr) throw new IllegalArgumentException("from is required in yyyy-MM-dd format")
if (!toDateStr) throw new IllegalArgumentException("to is required in yyyy-MM-dd format")
if (!nsConfigId) throw new IllegalArgumentException("nsRestletConfigId is required")
if (!comparisonRuleSetIdToUse) throw new IllegalArgumentException("comparisonRuleSetId is required")
if (!(fromDateStr ==~ /\d{4}-\d{2}-\d{2}/)) throw new IllegalArgumentException("from must be yyyy-MM-dd")
if (!(toDateStr ==~ /\d{4}-\d{2}-\d{2}/)) throw new IllegalArgumentException("to must be yyyy-MM-dd")

LocalDate fromDate = LocalDate.parse(fromDateStr)
LocalDate toDate = LocalDate.parse(toDateStr)
if (toDate.isBefore(fromDate)) throw new IllegalArgumentException("to date must be greater than or equal to from date")

def comparisonRuleSetExists = ec.entity.find("darpan.rule.RuleSet")
        .condition("ruleSetId", comparisonRuleSetIdToUse)
        .useCache(false)
        .one()
if (!comparisonRuleSetExists) {
    throw new IllegalArgumentException("comparisonRuleSetId ${comparisonRuleSetIdToUse} not found. Create it in Rule Engine.")
}

safeFieldExpr(nsItemIdFieldExpr, "discrepancyNsItemIdField")
safeFieldExpr(nsLocationIdFieldExpr, "discrepancyLocationIdField")
safeFieldExpr(omsItemIdFieldExpr, "omsItemIdField")
safeFieldExpr(omsLocationIdFieldExpr, "omsLocationIdField")
if (omsTxnDateFieldExpr) safeFieldExpr(omsTxnDateFieldExpr, "omsTxnDateField")
if (omsQuantityFieldExpr) safeFieldExpr(omsQuantityFieldExpr, "omsQuantityField")
safeFieldExpr(compareStatusFieldName, "compareStatusField")

String refType = refTypeRaw?.toUpperCase() ?: detectFileTypeFromLocation(refLocation)
String omsType = omsTypeRaw?.toUpperCase() ?: detectFileTypeFromLocation(omsDetailLocation)
if (!refType || !["CSV", "JSON"].contains(refType)) throw new IllegalArgumentException("referenceFileType must be CSV or JSON (or inferable by extension)")
if (!omsType || !["CSV", "JSON"].contains(omsType)) throw new IllegalArgumentException("omsDetailFileType must be CSV or JSON (or inferable by extension)")

String refPath = resolvePath(refLocation)
String omsPath = resolvePath(omsDetailLocation)

SparkSession spark = null
try {
    spark = SparkSession.builder()
            .appName(normalize(sparkAppName) ?: "InventoryReferenceRetrieval")
            .master(normalize(sparkMaster) ?: "local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()

    Dataset discrepancyDf = (refType == "JSON")
            ? spark.read().option("multiLine", "true").json(refPath)
            : spark.read().option("header", refHasHeader.toString()).option("multiLine", "true").csv(refPath)

    Dataset omsDf = (omsType == "JSON")
            ? spark.read().option("multiLine", "true").json(omsPath)
            : spark.read().option("header", omsHasHeader.toString()).option("multiLine", "true").csv(omsPath)

    Dataset pairDf = discrepancyDf
            .selectExpr(
                    "cast(${nsItemIdFieldExpr} as string) as ns_item_id",
                    "cast(${nsLocationIdFieldExpr} as string) as ns_location_id"
            )
            .filter("ns_item_id IS NOT NULL AND length(trim(ns_item_id)) > 0 AND ns_location_id IS NOT NULL AND length(trim(ns_location_id)) > 0")
            .distinct()
            .orderBy("ns_item_id", "ns_location_id")

    referenceItemCount = pairDf.count() as int
    if (referenceItemCount == 0) {
        throw new IllegalArgumentException("No usable discrepancy rows found in ${refLocation}")
    }

    Dataset finalPairDf = pairDf
    if (maxItemsToProcess > 0 && maxItemsToProcess < referenceItemCount) {
        finalPairDf = pairDf.limit(maxItemsToProcess)
        warningList.add("Processing limited to ${maxItemsToProcess} rows out of ${referenceItemCount} discrepancy rows.")
    }

    List<Map> itemPairs = finalPairDf.collectAsList().collect { Row row ->
        String itemVal = normalize(row.getAs("ns_item_id"))
        String locVal = normalize(row.getAs("ns_location_id"))
        [
                pairId    : "${itemVal}|${locVal}",
                itemId    : itemVal,
                locationId: locVal
        ]
    }

    Dataset discrepancyRowsDf = discrepancyDf
            .selectExpr(
                    "cast(${nsItemIdFieldExpr} as string) as ns_item_id",
                    "cast(${nsLocationIdFieldExpr} as string) as ns_location_id",
                    "to_json(struct(*)) as row_json"
            )
            .filter("ns_item_id IS NOT NULL AND length(trim(ns_item_id)) > 0 AND ns_location_id IS NOT NULL AND length(trim(ns_location_id)) > 0")

    Map<String, List<Map>> discrepancyByPair = [:].withDefault { [] }
    discrepancyRowsDf.collectAsList().each { Row row ->
        String key = "${normalize(row.getAs('ns_item_id'))}|${normalize(row.getAs('ns_location_id'))}"
        discrepancyByPair[key] << parseRowJson(normalize(row.getAs("row_json")))
    }

    List<String> omsSelectExpr = [
            "cast(${omsItemIdFieldExpr} as string) as oms_item_id",
            "cast(${omsLocationIdFieldExpr} as string) as oms_location_id",
            "to_json(struct(*)) as row_json"
    ]
    if (omsTxnDateFieldExpr) omsSelectExpr.add("cast(${omsTxnDateFieldExpr} as string) as oms_txn_date")
    if (omsQuantityFieldExpr) omsSelectExpr.add("cast(${omsQuantityFieldExpr} as string) as oms_qty")

    Dataset omsRowsDf = omsDf
            .selectExpr(omsSelectExpr as String[])
            .filter("oms_item_id IS NOT NULL AND length(trim(oms_item_id)) > 0 AND oms_location_id IS NOT NULL AND length(trim(oms_location_id)) > 0")

    Map<String, List<Map>> omsRowsByPair = [:].withDefault { [] }
    omsRowsDf.collectAsList().each { Row row ->
        String key = "${normalize(row.getAs('oms_item_id'))}|${normalize(row.getAs('oms_location_id'))}"
        Map payload = parseRowJson(normalize(row.getAs("row_json")))
        payload.__omsTxnDate = normalize(row.getAs("oms_txn_date"))
        payload.__omsQty = normalize(row.getAs("oms_qty"))
        omsRowsByPair[key] << payload
    }

    Map<String, Map> nsResultByPair = [:]
    List<Map> nsChunkResultsList = []
    int totalRetries = 0
    String nsDataSourceValue = "FETCHED"

    if (persistedNsOutputLocationValue) {
        String persistedPath = resolvePath(persistedNsOutputLocationValue)
        Object persistedPayload = null
        try {
            def persistedRef = ec.resource.getLocationReference(persistedNsOutputLocationValue)
            if (persistedRef != null && persistedRef.getExists()) {
                persistedPayload = new JsonSlurper().parseText(persistedRef.getText())
            } else {
                File persistedFile = new File(persistedPath)
                if (!persistedFile.exists()) {
                    throw new IllegalArgumentException("Persisted NS output file not found at ${persistedPath}")
                }
                persistedPayload = new JsonSlurper().parse(persistedFile)
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to parse persistedNsOutputLocation ${persistedNsOutputLocationValue}: ${normalize(e.message) ?: e.class.simpleName}")
        }

        List persistedRows = []
        if (persistedPayload instanceof Map) {
            Map payloadMap = (Map) persistedPayload
            if (payloadMap.rows instanceof List) persistedRows = (List) payloadMap.rows
            else if (payloadMap.itemResults instanceof List) persistedRows = (List) payloadMap.itemResults
            else throw new IllegalArgumentException("Persisted NS payload must include rows[] (NS output) or itemResults[] (summary)")
        } else if (persistedPayload instanceof List) {
            persistedRows = (List) persistedPayload
        } else {
            throw new IllegalArgumentException("Persisted NS payload must be a JSON object or list")
        }

        persistedRows.each { Object rowObj ->
            if (!(rowObj instanceof Map)) return
            Map row = (Map) rowObj
            String rowPairId = normalize(row.pairId)
            String rowItemId = normalize(row.itemId ?: row.nsItemId)
            String rowLocationId = normalize(row.locationId ?: row.nsLocationId)
            String pairKey = rowPairId ?: ((rowItemId && rowLocationId) ? "${rowItemId}|${rowLocationId}" : null)
            if (!pairKey) return

            String fallbackItemId = rowItemId
            String fallbackLocationId = rowLocationId
            if ((!fallbackItemId || !fallbackLocationId) && pairKey.contains("|")) {
                int pipeIdx = pairKey.indexOf("|")
                if (!fallbackItemId) fallbackItemId = pairKey.substring(0, pipeIdx)
                if (!fallbackLocationId) fallbackLocationId = pairKey.substring(pipeIdx + 1)
            }

            List rowRecords = []
            if (row.records instanceof List) rowRecords = (List) row.records
            else if (row.nsRecords instanceof List) rowRecords = (List) row.nsRecords
            int rowCount = row.recordCount != null
                    ? toInt(row.recordCount)
                    : (row.nsRecordCount != null ? toInt(row.nsRecordCount) : rowRecords.size())

            nsResultByPair[pairKey] = [
                    pairId      : pairKey,
                    itemId      : fallbackItemId,
                    locationId  : fallbackLocationId,
                    status      : normalize(row.status ?: row.nsStatus) ?: "OK",
                    errorMessage: normalize(row.errorMessage ?: row.error ?: row.nsError),
                    recordCount : rowCount,
                    records     : rowRecords
            ]
        }

        warningList.add("Reusing persisted NS data from ${persistedNsOutputLocationValue}; loaded ${nsResultByPair.size()} pair results.")
        nsDataSourceValue = "REUSED_PERSISTED"
    }

    List<Map> pairsToFetch = itemPairs.findAll { Map pair -> !nsResultByPair.containsKey(pair.pairId) }
    if (pairsToFetch && persistedNsOutputLocationValue && !fetchMissingNsPairsEnabled) {
        warningList.add("Persisted NS data missing ${pairsToFetch.size()} pair(s); fetchMissingNsPairs=false so missing pairs were marked as ERROR.")
        pairsToFetch.each { Map pair ->
            nsResultByPair[pair.pairId] = [
                    pairId      : pair.pairId,
                    itemId      : pair.itemId,
                    locationId  : pair.locationId,
                    status      : "ERROR",
                    errorMessage: "Missing from persisted NS data and fetchMissingNsPairs is false",
                    recordCount : 0,
                    records     : []
            ]
        }
        nsChunkResultsList << [
                chunkIndex: 0,
                pairCount : itemPairs.size(),
                attempts  : 0,
                status    : "REUSED_WITH_GAPS",
                error     : "Missing persisted pairs were not fetched",
                pairIds   : itemPairs.collect { it.pairId }
        ]
    } else {
        if (persistedNsOutputLocationValue && !pairsToFetch) {
            nsChunkResultsList << [
                    chunkIndex: 0,
                    pairCount : itemPairs.size(),
                    attempts  : 0,
                    status    : "REUSED",
                    error     : null,
                    pairIds   : itemPairs.collect { it.pairId }
            ]
        }

        if (pairsToFetch) {
            if (persistedNsOutputLocationValue && fetchMissingNsPairsEnabled) {
                warningList.add("Persisted NS data missing ${pairsToFetch.size()} pair(s); fetching only missing pairs from NetSuite.")
                nsDataSourceValue = "MIXED_REUSED_AND_FETCHED"
            } else {
                nsDataSourceValue = "FETCHED"
            }

            int chunkIndexBase = nsChunkResultsList.size()
            List<List<Map>> chunks = pairsToFetch.collate(chunkSize)
            chunks.eachWithIndex { List<Map> chunk, int chunkIdx ->
                int effectiveChunkIndex = chunkIndexBase + chunkIdx
                int attempt = 0
                boolean done = false
                Map chunkResult = null
                Exception lastFailure = null

                while (!done) {
                    attempt++
                    try {
                        Map bulkOut = ec.service.sync()
                                .name("reconciliation.NetSuiteInventoryServices.fetch#NsInventoryAdjustmentsBulk")
                                .parameters([
                                        nsRestletConfigId: nsConfigId,
                                        itemPairs        : chunk,
                                        from             : fromDateStr,
                                        to               : toDateStr,
                                        strictMaxPairs   : true
                                ])
                                .call()

                        List pairResults = (bulkOut.pairResults ?: []) as List
                        pairResults.each { Object rowObj ->
                            Map row = (rowObj instanceof Map) ? ((Map) rowObj) : [:]
                            String rowPairId = normalize(row.pairId) ?: "${normalize(row.itemId)}|${normalize(row.locationId)}"
                            if (rowPairId) nsResultByPair[rowPairId] = row
                        }

                        chunkResult = [
                                chunkIndex: effectiveChunkIndex,
                                pairCount : chunk.size(),
                                attempts  : attempt,
                                status    : "SUCCESS",
                                error     : null,
                                pairIds   : chunk.collect { it.pairId }
                        ]
                        done = true
                    } catch (Exception e) {
                        lastFailure = e
                        boolean retryable = isRetryableFailure(e.message)
                        if (retryable && attempt <= maxRetries) {
                            totalRetries++
                            long sleepMs = (retryBackoffMs * backoffMultiplier.pow(attempt - 1)).longValue()
                            if (sleepMs > 0L) Thread.sleep(sleepMs)
                            continue
                        }

                        chunkResult = [
                                chunkIndex: effectiveChunkIndex,
                                pairCount : chunk.size(),
                                attempts  : attempt,
                                status    : "FAILED",
                                error     : normalize(e.message) ?: "Chunk failed",
                                pairIds   : chunk.collect { it.pairId }
                        ]

                        if (continueOnChunkFailure) {
                            chunk.each { Map pair ->
                                nsResultByPair[pair.pairId] = [
                                        pairId      : pair.pairId,
                                        itemId      : pair.itemId,
                                        locationId  : pair.locationId,
                                        status      : "ERROR",
                                        errorMessage: chunkResult.error,
                                        recordCount : 0,
                                        records     : []
                                ]
                            }
                            warningList.add("NS chunk ${effectiveChunkIndex} failed after ${attempt} attempt(s): ${chunkResult.error}")
                            done = true
                        } else {
                            throw lastFailure
                        }
                    }
                }

                nsChunkResultsList << chunkResult
                if (nsDelayMsToUse > 0 && chunkIdx < chunks.size() - 1) Thread.sleep(nsDelayMsToUse as long)
            }
        }
    }

    List<Map> itemResultRows = itemPairs.collect { Map pair ->
        String pairKey = pair.pairId
        List<Map> discrepancyRows = (discrepancyByPair[pairKey] ?: []) as List<Map>
        List<Map> omsRows = (omsRowsByPair[pairKey] ?: []) as List<Map>
        Map nsRow = (nsResultByPair[pairKey] ?: [
                pairId      : pairKey,
                itemId      : pair.itemId,
                locationId  : pair.locationId,
                status      : "ERROR",
                errorMessage: "No NS result returned",
                recordCount : 0,
                records     : []
        ]) as Map

        List<Map> nsRecords = (nsRow.records instanceof List) ? (List<Map>) nsRow.records : []
        int nsCount = nsRow.recordCount != null ? toInt(nsRow.recordCount) : nsRecords.size()

        BigDecimal omsQtyTotal = omsRows.collect { Map r -> toDecimal(r.__omsQty) }.inject(BigDecimal.ZERO) { a, b -> a + b }
        String omsMinDate = omsRows.collect { Map r -> normalize(r.__omsTxnDate) }.findAll { it }.sort().with { it ? it.first() : null }
        String omsMaxDate = omsRows.collect { Map r -> normalize(r.__omsTxnDate) }.findAll { it }.sort().with { it ? it.last() : null }

        BigDecimal nsQtyTotal = nsRecords.collect { Map r -> extractQty(r) }.inject(BigDecimal.ZERO) { a, b -> a + b }
        List<String> nsDates = nsRecords.collect { Map r -> extractTxnDate(r) }.findAll { it }
        nsDates = nsDates.sort()
        String nsMinDate = nsDates ? nsDates.first() : null
        String nsMaxDate = nsDates ? nsDates.last() : null

        int omsCount = omsRows.size()
        String nsStatus = normalize(nsRow.status) ?: "OK"

        [
                pairId            : pairKey,
                itemId            : pair.itemId,
                locationId        : pair.locationId,
                nsItemId          : pair.itemId,
                nsLocationId      : pair.locationId,
                readDbItemId      : pair.itemId,
                readDbLocationId  : pair.locationId,
                discrepancy       : discrepancyRows ? discrepancyRows[0] : [:],
                discrepancyRows   : discrepancyRows,
                omsDetailRows     : omsRows,
                nsStatus          : nsStatus,
                nsRecordCount     : nsCount,
                nsError           : normalize(nsRow.errorMessage ?: nsRow.error),
                nsRecords         : nsRecords,
                readDbStatus      : (omsCount > 0 ? "OK" : "MISSING"),
                readDbRecordCount : omsCount,
                readDbError       : null,
                readDbRecords     : omsRows,
                omsRecordCount    : omsCount,
                omsQuantityTotal  : omsQtyTotal,
                omsMinTxnDate     : omsMinDate,
                omsMaxTxnDate     : omsMaxDate,
                nsQuantityTotal   : nsQtyTotal,
                nsMinTxnDate      : nsMinDate,
                nsMaxTxnDate      : nsMaxDate,
                recordCountDelta  : nsCount - omsCount,
                quantityDelta     : nsQtyTotal.subtract(omsQtyTotal),
                (compareStatusFieldName): null,
                (reasonCodeFieldName)  : null,
                (reasonTextFieldName)  : null,
                (matchedRuleIdsFieldName): []
        ]
    }

    Map ruleOut = ec.service.sync()
            .name("reconciliation.ReconciliationInventoryServices.evaluate#InventoryAdjustmentComparisonRules")
            .parameters([
                    ruleSetId     : comparisonRuleSetIdToUse,
                    dataList      : itemResultRows,
                    returnAllFacts: true
            ])
            .call()
    if (ruleOut.error) {
        throw new IllegalArgumentException("Comparison rules failed for ruleSet ${comparisonRuleSetIdToUse}: ${normalize(ruleOut.error)}")
    }

    List<Map> ruledRows = ((ruleOut.results ?: []) as List).collect { Object rowObj ->
        (rowObj instanceof Map) ? new LinkedHashMap((Map) rowObj) : [:]
    }
    if (ruledRows.size() != itemResultRows.size()) {
        throw new IllegalArgumentException("Comparison rules returned ${ruledRows.size()} rows; expected ${itemResultRows.size()}")
    }

    ruledRows.each { Map row ->
        String statusValue = normalize(row[compareStatusFieldName])
        if (!normalize(row[reasonCodeFieldName]) && statusValue) {
            row[reasonCodeFieldName] = statusValue
            row[reasonTextFieldName] = normalize(row[reasonTextFieldName]) ?: "Derived from compareStatus ${statusValue}"
        }
        Object matchedIds = row[matchedRuleIdsFieldName]
        if (!(matchedIds instanceof List)) {
            if (matchedIds == null) row[matchedRuleIdsFieldName] = []
            else row[matchedRuleIdsFieldName] = [matchedIds]
        }
    }

    processedItemCount = ruledRows.size()
    nsRecordCount = ruledRows.collect { toInt(it.nsRecordCount) }.sum() as int
    readDbRecordCount = ruledRows.collect { toInt(it.readDbRecordCount) }.sum() as int
    missingInNsCount = ruledRows.count { normalize(it[compareStatusFieldName]) == missingInNsStatusValue } as int
    missingInReadDbCount = ruledRows.count { normalize(it[compareStatusFieldName]) == missingInReadDbStatusValue } as int
    countMismatchCount = ruledRows.count { normalize(it[compareStatusFieldName]) == countMismatchStatusValue } as int
    matchedCount = ruledRows.count { normalize(it[compareStatusFieldName]) == matchedStatusValue } as int
    noRecordCount = ruledRows.count { normalize(it[compareStatusFieldName]) == noRecordStatusValue } as int
    errorItemCount = ruledRows.count { normalize(it[compareStatusFieldName]) == errorStatusValue } as int

    Map<String, Integer> reasonCountMap = [:].withDefault { 0 }
    ruledRows.each { Map row ->
        String reasonCodeValue = normalize(row[reasonCodeFieldName])
        if (reasonCodeValue) reasonCountMap[reasonCodeValue] = reasonCountMap[reasonCodeValue] + 1
    }
    explainedItemCount = reasonCountMap.values().sum() ?: 0
    unexplainedItemCount = processedItemCount - explainedItemCount
    reasonCounts = reasonCountMap.collect { k, v -> [reasonCode: k, count: v] }

    runId = runIdToUse
    nsChunkCount = nsChunkResultsList.size()
    nsChunkSuccessCount = nsChunkResultsList.count { it.status == "SUCCESS" } as int
    nsChunkFailureCount = nsChunkResultsList.count { it.status == "FAILED" } as int
    nsTotalRetryCount = totalRetries
    nsDataSource = nsDataSourceValue
    nsChunkResults = nsChunkResultsList

    String outputBaseLocation = normalize(outputLocation) ?: "runtime://tmp/reconciliation/inventory/retrieval"
    def outputRef = ec.resource.getLocationReference(outputBaseLocation)
    File outputDir = outputRef?.getFile()
    if (outputDir == null) {
        outputDir = new File(ec.factory.getRuntimePath(), outputBaseLocation.replace("runtime://", ""))
    }
    if (!outputDir.exists() && !outputDir.mkdirs()) {
        throw new IllegalArgumentException("Unable to create output directory at ${outputDir.absolutePath}")
    }

    String baseName = (normalize(outputBaseName)?.replaceAll(/[^A-Za-z0-9._-]/, "-")) ?: "inventory-adjustments"
    String timestamp = ec.l10n.format(ec.user.nowTimestamp, "yyyyMMdd-HHmmss")

    Map nsDoc = [
            metadata: [
                    source          : "NS",
                    nsDataSource    : nsDataSourceValue,
                    persistedNsOutputLocation: persistedNsOutputLocationValue,
                    runId           : runIdToUse,
                    nsRestletConfigId: nsConfigId,
                    from            : fromDateStr,
                    to              : toDateStr,
                    generatedAt     : ec.user.nowTimestamp?.toString()
            ],
            rows    : itemPairs.collect { Map pair ->
                Map nsRow = (nsResultByPair[pair.pairId] ?: [:]) as Map
                [
                        pairId      : pair.pairId,
                        itemId      : pair.itemId,
                        locationId  : pair.locationId,
                        status      : normalize(nsRow.status) ?: "ERROR",
                        error       : normalize(nsRow.errorMessage ?: nsRow.error),
                        recordCount : toInt(nsRow.recordCount ?: ((nsRow.records instanceof List) ? ((List) nsRow.records).size() : 0)),
                        records     : (nsRow.records instanceof List) ? nsRow.records : []
                ]
            }
    ]

    Map readDbDoc = [
            metadata: [
                    source      : "OMS_CSV",
                    runId       : runIdToUse,
                    from        : fromDateStr,
                    to          : toDateStr,
                    generatedAt : ec.user.nowTimestamp?.toString()
            ],
            rows    : itemPairs.collect { Map pair ->
                List<Map> rows = (omsRowsByPair[pair.pairId] ?: []) as List<Map>
                [
                        pairId      : pair.pairId,
                        itemId      : pair.itemId,
                        locationId  : pair.locationId,
                        status      : rows ? "OK" : "MISSING",
                        error       : null,
                        recordCount : rows.size(),
                        records     : rows
                ]
            }
    ]

    Map summaryDoc = [
            metadata : [
                    runId               : runIdToUse,
                    referenceFileLocation: refLocation,
                    omsDetailFileLocation: omsDetailLocation,
                    referenceFileType   : refType,
                    omsDetailFileType   : omsType,
                    discrepancyNsItemIdField: nsItemIdFieldExpr,
                    discrepancyLocationIdField: nsLocationIdFieldExpr,
                    omsItemIdField      : omsItemIdFieldExpr,
                    omsLocationIdField  : omsLocationIdFieldExpr,
                    omsTxnDateField     : omsTxnDateFieldExpr,
                    omsQuantityField    : omsQuantityFieldExpr,
                    comparisonRuleSetId : comparisonRuleSetIdToUse,
                    nsDataSource        : nsDataSourceValue,
                    persistedNsOutputLocation: persistedNsOutputLocationValue,
                    fetchMissingNsPairs : fetchMissingNsPairsEnabled,
                    compareStatusField  : compareStatusFieldName,
                    reasonCodeField     : reasonCodeFieldName,
                    reasonTextField     : reasonTextFieldName,
                    matchedRuleIdsField : matchedRuleIdsFieldName,
                    from                : fromDateStr,
                    to                  : toDateStr,
                    generatedAt         : ec.user.nowTimestamp?.toString()
            ],
            summary  : [
                    referenceItemCount   : referenceItemCount,
                    processedItemCount   : processedItemCount,
                    nsRecordCount        : nsRecordCount,
                    readDbRecordCount    : readDbRecordCount,
                    missingInNsCount     : missingInNsCount,
                    missingInReadDbCount : missingInReadDbCount,
                    countMismatchCount   : countMismatchCount,
                    matchedCount         : matchedCount,
                    noRecordCount        : noRecordCount,
                    errorItemCount       : errorItemCount,
                    explainedItemCount   : explainedItemCount,
                    unexplainedItemCount : unexplainedItemCount,
                    reasonCounts         : reasonCounts,
                    nsChunkCount         : nsChunkCount,
                    nsChunkSuccessCount  : nsChunkSuccessCount,
                    nsChunkFailureCount  : nsChunkFailureCount,
                    nsTotalRetryCount    : nsTotalRetryCount
            ],
            nsChunkResults: nsChunkResultsList,
            itemResults: ruledRows,
            warnings: warningList.unique()
    ]

    File nsFile = writeOutputJson(outputDir, "ns", baseName, timestamp, nsDoc)
    File readDbFile = writeOutputJson(outputDir, "read-db", baseName, timestamp, readDbDoc)
    File summaryFile = writeOutputJson(outputDir, "summary", baseName, timestamp, summaryDoc)

    nsOutputLocation = nsFile.absolutePath
    readDbOutputLocation = readDbFile.absolutePath
    summaryLocation = summaryFile.absolutePath
    itemResults = ruledRows
    processingWarnings = warningList.unique().collect { [warningMessage: it] }

    logger.info("Two-CSV inventory retrieval complete runId={} processedItems={} chunkCount={} retries={} summary={}",
            runIdToUse, processedItemCount, nsChunkCount, nsTotalRetryCount, summaryLocation)
} finally {
    try { spark?.stop() } catch (Exception ignored) {}
}
