import darpan.facade.common.FacadeSupport
import groovy.json.JsonSlurper

String nsRestletConfigIdValue = FacadeSupport.normalize(nsRestletConfigId)
String nsDescriptionValue = FacadeSupport.normalize(description)
String nsEndpointUrlValue = FacadeSupport.normalize(endpointUrl)
String nsHttpMethodValue = FacadeSupport.normalize(httpMethod)?.toUpperCase() ?: "POST"
String nsAuthConfigIdValue = FacadeSupport.normalize(nsAuthConfigId)
String nsHeadersJsonValue = FacadeSupport.normalize(headersJson)
Integer nsConnectTimeoutValue = FacadeSupport.normalizeInt(connectTimeoutSeconds, 30)
Integer nsReadTimeoutValue = FacadeSupport.normalizeInt(readTimeoutSeconds, 60)
String nsIsActiveValue = FacadeSupport.normalizeBool(isActive, true) ? "Y" : "N"

if (!nsRestletConfigIdValue) ec.message.addError("Endpoint Config ID is required.")
if (!nsEndpointUrlValue) ec.message.addError("Endpoint URL is required.")
if (!["POST", "PUT", "GET"].contains(nsHttpMethodValue)) {
    ec.message.addError("HTTP Method must be POST, PUT, or GET.")
}
if (!nsAuthConfigIdValue) ec.message.addError("Auth Config ID is required.")
if (nsConnectTimeoutValue <= 0) nsConnectTimeoutValue = 30
if (nsReadTimeoutValue <= 0) nsReadTimeoutValue = 60

if (nsHeadersJsonValue) {
    try {
        def parsedHeaders = new JsonSlurper().parseText(nsHeadersJsonValue)
        if (!(parsedHeaders instanceof Map)) {
            ec.message.addError("Headers JSON must be a JSON object.")
        }
    } catch (Exception e) {
        ec.message.addError("Headers JSON is invalid: ${e.message}")
    }
}

if (!ec.message.hasError()) {
    def authCfg = ec.entity.find("darpan.reconciliation.NsAuthConfig")
        .condition("nsAuthConfigId", nsAuthConfigIdValue)
        .useCache(false)
        .one()
    if (!authCfg) {
        ec.message.addError("NsAuthConfig ${nsAuthConfigIdValue} not found.")
    } else if ((authCfg.isActive ?: "Y").toString().equalsIgnoreCase("N")) {
        ec.message.addError("NsAuthConfig ${nsAuthConfigIdValue} is inactive.")
    }

    if (!ec.message.hasError()) {
        Map endpointMap = [
            nsRestletConfigId: nsRestletConfigIdValue,
            description: nsDescriptionValue,
            endpointUrl: nsEndpointUrlValue,
            httpMethod: nsHttpMethodValue,
            nsAuthConfigId: nsAuthConfigIdValue,
            headersJson: nsHeadersJsonValue,
            connectTimeoutSeconds: nsConnectTimeoutValue,
            readTimeoutSeconds: nsReadTimeoutValue,
            isActive: nsIsActiveValue,
            authType: null,
            username: null,
            password: null,
            apiToken: null,
            tokenUrl: null,
            clientId: null,
            certId: null,
            privateKeyPem: null,
            scope: null,
        ]

        ec.service.sync().name("store#darpan.reconciliation.NsRestletConfig").parameters(endpointMap).call()

        savedRestletConfig = [
            nsRestletConfigId: nsRestletConfigIdValue,
            description: nsDescriptionValue,
            endpointUrl: nsEndpointUrlValue,
            httpMethod: nsHttpMethodValue,
            nsAuthConfigId: nsAuthConfigIdValue,
            headersJson: nsHeadersJsonValue,
            connectTimeoutSeconds: nsConnectTimeoutValue,
            readTimeoutSeconds: nsReadTimeoutValue,
            isActive: nsIsActiveValue,
        ]

        if (!ec.message.hasError()) {
            ec.message.addMessage("Saved NS endpoint config ${nsRestletConfigIdValue}.")
        }
    }
}

Map envelope = FacadeSupport.envelope(ec)
ok = envelope.ok
messages = envelope.messages
errors = envelope.errors
