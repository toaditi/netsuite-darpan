import darpan.facade.common.FacadeSupport

int page = Math.max(0, FacadeSupport.normalizeInt(pageIndex, 0))
int size = Math.max(1, Math.min(200, FacadeSupport.normalizeInt(pageSize, 20)))

Map authById = [:]
(ec.entity.find("darpan.reconciliation.NsAuthConfig")
    .useCache(false)
    .list() ?: []).each { cfg ->
    authById[cfg.nsAuthConfigId] = [
        description: cfg.description,
        authType: cfg.authType,
        isActive: cfg.isActive,
    ]
}

List<Map> rows = []
(ec.entity.find("darpan.reconciliation.NsRestletConfig")
    .useCache(false)
    .orderBy("description,nsRestletConfigId")
    .list() ?: []).each { cfg ->
    def authMeta = authById[cfg.nsAuthConfigId]
    rows.add([
        nsRestletConfigId: cfg.nsRestletConfigId,
        description: cfg.description,
        endpointUrl: cfg.endpointUrl,
        httpMethod: cfg.httpMethod ?: "POST",
        nsAuthConfigId: cfg.nsAuthConfigId,
        authDescription: authMeta?.description,
        authType: authMeta?.authType,
        authIsActive: authMeta?.isActive ?: "Y",
        headersJson: cfg.headersJson,
        connectTimeoutSeconds: cfg.connectTimeoutSeconds ?: 30,
        readTimeoutSeconds: cfg.readTimeoutSeconds ?: 60,
        isActive: cfg.isActive ?: "Y",
    ])
}

String search = FacadeSupport.normalize(query)?.toLowerCase()
List<Map> filtered = search ? rows.findAll { row ->
    [row.nsRestletConfigId, row.description, row.endpointUrl, row.nsAuthConfigId, row.authType].any { it?.toString()?.toLowerCase()?.contains(search) }
} : rows

int totalCount = filtered.size()
int fromIndex = Math.min(page * size, totalCount)
int toIndex = Math.min(fromIndex + size, totalCount)
restletConfigs = filtered.subList(fromIndex, toIndex)

pagination = [
    pageIndex: page,
    pageSize: size,
    totalCount: totalCount,
    pageCount: Math.max(1, Math.ceil(totalCount / (double) size) as int)
]

Map envelope = FacadeSupport.envelope(ec)
ok = envelope.ok
messages = envelope.messages
errors = envelope.errors
