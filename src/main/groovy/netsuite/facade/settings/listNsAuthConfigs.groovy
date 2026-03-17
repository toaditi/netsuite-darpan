import darpan.facade.common.FacadeSupport

int page = Math.max(0, FacadeSupport.normalizeInt(pageIndex, 0))
int size = Math.max(1, Math.min(200, FacadeSupport.normalizeInt(pageSize, 20)))

List<Map> rows = []
(ec.entity.find("darpan.reconciliation.NsAuthConfig")
    .useCache(false)
    .orderBy("description,nsAuthConfigId")
    .list() ?: []).each { cfg ->
    rows.add([
        nsAuthConfigId: cfg.nsAuthConfigId,
        description: cfg.description,
        authType: cfg.authType ?: "NONE",
        username: cfg.username,
        tokenUrl: cfg.tokenUrl,
        clientId: cfg.clientId,
        certId: cfg.certId,
        scope: cfg.scope ?: "restlets rest_webservices",
        isActive: cfg.isActive ?: "Y",
        hasPassword: !!cfg.password,
        hasApiToken: !!cfg.apiToken,
        hasPrivateKeyPem: !!cfg.privateKeyPem,
    ])
}

String search = FacadeSupport.normalize(query)?.toLowerCase()
List<Map> filtered = search ? rows.findAll { row ->
    [row.nsAuthConfigId, row.description, row.authType, row.username].any { it?.toString()?.toLowerCase()?.contains(search) }
} : rows

int totalCount = filtered.size()
int fromIndex = Math.min(page * size, totalCount)
int toIndex = Math.min(fromIndex + size, totalCount)
authConfigs = filtered.subList(fromIndex, toIndex)

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
