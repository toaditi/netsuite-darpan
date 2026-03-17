/**
 * NetSuite SuiteScript 2.1 Restlet
 * Bulk inventory adjustment retrieval for up to 100 item/location pairs.
 */
define(['N/search', 'N/runtime'], (search, runtime) => {
  const MAX_PAIRS = 100

  const toStr = (v) => (v === null || v === undefined ? '' : String(v).trim())
  const toInt = (v) => {
    const n = Number(v)
    return Number.isFinite(n) ? n : null
  }

  const buildError = (pair, code, message, retryable = false) => ({
    pairId: pair.pairId,
    itemId: pair.itemId,
    locationId: pair.locationId,
    status: 'ERROR',
    errorCode: code,
    errorMessage: message,
    retryable,
    recordCount: 0,
    records: []
  })

  const normalizeRequest = (payload) => {
    if (!payload || typeof payload !== 'object') {
      throw new Error('Request payload must be a JSON object')
    }

    const from = toStr(payload.from)
    const to = toStr(payload.to)
    if (!/^\d{4}-\d{2}-\d{2}$/.test(from)) throw new Error('from must be yyyy-MM-dd')
    if (!/^\d{4}-\d{2}-\d{2}$/.test(to)) throw new Error('to must be yyyy-MM-dd')

    const pairs = Array.isArray(payload.pairs) ? payload.pairs : []
    if (!pairs.length) throw new Error('pairs is required and must contain at least one row')
    if (pairs.length > MAX_PAIRS) throw new Error(`pairs cannot exceed ${MAX_PAIRS}`)

    const seen = new Set()
    const normalized = pairs.map((row, idx) => {
      const itemId = toStr(row.itemId)
      const locationId = toStr(row.locationId)
      if (!itemId) throw new Error(`pairs[${idx}] missing itemId`)
      if (!locationId) throw new Error(`pairs[${idx}] missing locationId`)

      const pairId = toStr(row.pairId) || `${itemId}|${locationId}`
      if (seen.has(pairId)) throw new Error(`Duplicate pairId: ${pairId}`)
      seen.add(pairId)

      return { pairId, itemId, locationId }
    })

    return {
      requestId: toStr(payload.requestId) || null,
      from,
      to,
      pairs: normalized,
      options: payload.options || {}
    }
  }

  const fetchTransactions = (pair, from, to) => {
    // Placeholder query shape. Adjust record type/columns to account-specific model.
    const itemId = toInt(pair.itemId)
    const locationId = toInt(pair.locationId)
    if (itemId === null || locationId === null) {
      return buildError(pair, 'INVALID_KEY', 'itemId and locationId must be numeric', false)
    }

    const txnSearch = search.create({
      type: search.Type.TRANSACTION,
      filters: [
        ['item.internalid', 'anyof', itemId], 'AND',
        ['inventorylocation.internalid', 'anyof', locationId], 'AND',
        ['trandate', 'within', from, to]
      ],
      columns: [
        search.createColumn({ name: 'internalid' }),
        search.createColumn({ name: 'trandate' }),
        search.createColumn({ name: 'type' }),
        search.createColumn({ name: 'quantity' })
      ]
    })

    const records = []
    txnSearch.run().each((row) => {
      records.push({
        transaction: {
          id: row.getValue({ name: 'internalid' }),
          date: row.getValue({ name: 'trandate' }),
          type: row.getText({ name: 'type' }) || row.getValue({ name: 'type' }),
          qty: row.getValue({ name: 'quantity' })
        }
      })
      return true
    })

    return {
      pairId: pair.pairId,
      itemId: pair.itemId,
      locationId: pair.locationId,
      status: 'OK',
      errorCode: null,
      errorMessage: null,
      retryable: false,
      recordCount: records.length,
      records
    }
  }

  const post = (payload) => {
    const req = normalizeRequest(payload)

    const results = req.pairs.map((pair) => {
      try {
        return fetchTransactions(pair, req.from, req.to)
      } catch (e) {
        return buildError(pair, 'PAIR_PROCESSING_FAILED', e.message || 'Unexpected pair failure', false)
      }
    })

    const successPairs = results.filter((r) => r.status !== 'ERROR').length
    const errorPairs = results.length - successPairs

    return {
      ok: true,
      requestId: req.requestId,
      environment: runtime.envType,
      summary: {
        requestedPairs: req.pairs.length,
        processedPairs: results.length,
        successPairs,
        errorPairs
      },
      results,
      errors: []
    }
  }

  return { post }
})
