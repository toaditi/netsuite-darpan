import org.slf4j.LoggerFactory

def logger = LoggerFactory.getLogger("netsuite.reconciliation.inventory.fetchNsInventoryAdjustments")

def normalize = { Object value -> value?.toString()?.trim() }

String pairItemId = normalize(itemId)
String pairLocationId = normalize(locationId)
if (!pairItemId) throw new IllegalArgumentException("itemId is required")
if (!pairLocationId) throw new IllegalArgumentException("locationId is required")

Map bulkOut = ec.service.sync()
        .name("reconciliation.NetSuiteInventoryServices.fetch#NsInventoryAdjustmentsBulk")
        .parameters([
                nsRestletConfigId: nsRestletConfigId,
                itemPairs        : [[pairId: "single", itemId: pairItemId, locationId: pairLocationId]],
                from             : from,
                to               : to,
                strictMaxPairs   : true
        ])
        .call()

statusCode = bulkOut.statusCode
responseBody = bulkOut.responseBody
processingWarnings = bulkOut.processingWarnings ?: []

Map firstResult = ((bulkOut.pairResults ?: []) as List).find { Map r ->
    normalize(r?.itemId) == pairItemId && normalize(r?.locationId) == pairLocationId
} as Map
if (!firstResult) {
    firstResult = ((bulkOut.pairResults ?: []) as List).find { true } as Map
}

if (firstResult?.status == "ERROR") {
    String errMsg = normalize(firstResult.errorMessage) ?: normalize(firstResult.error) ?: "Unknown NetSuite pair error"
    throw new IllegalArgumentException("NetSuite restlet response error: ${errMsg}")
}

List pairRecords = (firstResult?.records ?: []) as List
records = pairRecords.collect { [record: it] }
recordCount = records.size()

logger.info("NetSuite single-pair fetch delegated to bulk service config={} itemId={} locationId={} records={}",
        normalize(nsRestletConfigId), pairItemId, pairLocationId, recordCount)
