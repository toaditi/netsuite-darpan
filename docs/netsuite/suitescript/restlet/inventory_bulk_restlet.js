/**
 * @NApiVersion 2.0
 * @NScriptType Restlet
 *
 * NetSuite Restlet for bulk inventory adjustment retrieval.
 * Request supports up to 100 item/location pairs per call.
 */
define(['N/search', 'N/runtime'], function(search, runtime) {
  var MAX_PAIRS = 100;

  function toStr(value) {
    return (value === null || value === undefined) ? '' : String(value).trim();
  }

  function toInt(value) {
    var num = Number(value);
    return isFinite(num) ? num : null;
  }

  function buildError(pair, code, message, retryable) {
    return {
      pairId: pair.pairId,
      itemId: pair.itemId,
      locationId: pair.locationId,
      status: 'ERROR',
      errorCode: code,
      errorMessage: message,
      retryable: !!retryable,
      recordCount: 0,
      records: []
    };
  }

  function normalizeRequest(payload) {
    var req = payload || {};
    if (Object.prototype.toString.call(req) !== '[object Object]') {
      throw new Error('Request payload must be a JSON object');
    }

    var fromDate = toStr(req.from);
    var toDate = toStr(req.to);

    if (!/^\d{4}-\d{2}-\d{2}$/.test(fromDate)) {
      throw new Error('from must be yyyy-MM-dd');
    }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(toDate)) {
      throw new Error('to must be yyyy-MM-dd');
    }

    var pairs = Array.isArray(req.pairs) ? req.pairs : [];
    if (!pairs.length) {
      throw new Error('pairs is required and must contain at least one row');
    }
    if (pairs.length > MAX_PAIRS) {
      throw new Error('pairs cannot exceed ' + MAX_PAIRS);
    }

    var normalized = [];
    var seen = {};
    var i;
    for (i = 0; i < pairs.length; i += 1) {
      var row = pairs[i] || {};
      var itemId = toStr(row.itemId);
      var locationId = toStr(row.locationId);
      if (!itemId) throw new Error('pairs[' + i + '] missing itemId');
      if (!locationId) throw new Error('pairs[' + i + '] missing locationId');

      var pairId = toStr(row.pairId) || (itemId + '|' + locationId);
      if (seen[pairId]) throw new Error('Duplicate pairId: ' + pairId);
      seen[pairId] = true;

      normalized.push({
        pairId: pairId,
        itemId: itemId,
        locationId: locationId
      });
    }

    return {
      requestId: toStr(req.requestId) || null,
      from: fromDate,
      to: toDate,
      pairs: normalized,
      options: req.options || {}
    };
  }

  function fetchTransactions(pair, fromDate, toDate) {
    var itemId = toInt(pair.itemId);
    var locationId = toInt(pair.locationId);
    if (itemId === null || locationId === null) {
      return buildError(pair, 'INVALID_KEY', 'itemId and locationId must be numeric', false);
    }

    var txnSearch = search.create({
      type: search.Type.TRANSACTION,
      filters: [
        ['item.internalid', 'anyof', itemId], 'AND',
        ['inventorylocation.internalid', 'anyof', locationId], 'AND',
        ['trandate', 'within', fromDate, toDate]
      ],
      columns: [
        search.createColumn({ name: 'internalid' }),
        search.createColumn({ name: 'trandate' }),
        search.createColumn({ name: 'type' }),
        search.createColumn({ name: 'quantity' })
      ]
    });

    var records = [];
    txnSearch.run().each(function(row) {
      records.push({
        transaction: {
          id: row.getValue({ name: 'internalid' }),
          date: row.getValue({ name: 'trandate' }),
          type: row.getText({ name: 'type' }) || row.getValue({ name: 'type' }),
          qty: row.getValue({ name: 'quantity' })
        }
      });
      return true;
    });

    return {
      pairId: pair.pairId,
      itemId: pair.itemId,
      locationId: pair.locationId,
      status: 'OK',
      errorCode: null,
      errorMessage: null,
      retryable: false,
      recordCount: records.length,
      records: records
    };
  }

  function post(payload) {
    var req = normalizeRequest(payload);
    var results = [];
    var i;

    for (i = 0; i < req.pairs.length; i += 1) {
      var pair = req.pairs[i];
      try {
        results.push(fetchTransactions(pair, req.from, req.to));
      } catch (e) {
        results.push(buildError(pair, 'PAIR_PROCESSING_FAILED', (e && e.message) || 'Unexpected pair failure', false));
      }
    }

    var successPairs = 0;
    for (i = 0; i < results.length; i += 1) {
      if (results[i].status !== 'ERROR') successPairs += 1;
    }

    return {
      ok: true,
      requestId: req.requestId,
      environment: runtime.envType,
      summary: {
        requestedPairs: req.pairs.length,
        processedPairs: results.length,
        successPairs: successPairs,
        errorPairs: results.length - successPairs
      },
      results: results,
      errors: []
    };
  }

  return { post: post };
});
