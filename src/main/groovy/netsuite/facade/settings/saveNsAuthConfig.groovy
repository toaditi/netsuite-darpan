import darpan.facade.common.FacadeSupport

String nsAuthConfigIdValue = FacadeSupport.normalize(nsAuthConfigId)
String nsAuthDescriptionValue = FacadeSupport.normalize(description)
String nsAuthTypeValue = FacadeSupport.normalize(authType)?.toUpperCase() ?: "NONE"
String nsUsernameValue = FacadeSupport.normalize(username)
String nsPasswordValue = FacadeSupport.normalize(password)
String nsApiTokenValue = FacadeSupport.normalize(apiToken)
String nsTokenUrlValue = FacadeSupport.normalize(tokenUrl)
String nsClientIdValue = FacadeSupport.normalize(clientId)
String nsCertIdValue = FacadeSupport.normalize(certId)
String nsScopeValue = FacadeSupport.normalize(scope) ?: "restlets rest_webservices"
String nsPrivateKeyPemValue = FacadeSupport.normalize(privateKeyPem)
String nsIsActiveValue = FacadeSupport.normalizeBool(isActive, true) ? "Y" : "N"

if (!nsAuthConfigIdValue) ec.message.addError("Auth Config ID is required.")
if (!["NONE", "BASIC", "BEARER", "OAUTH2_M2M_JWT"].contains(nsAuthTypeValue)) {
    ec.message.addError("Auth Type must be NONE, BASIC, BEARER, or OAUTH2_M2M_JWT.")
}
if (nsAuthTypeValue == "BASIC" && !nsUsernameValue) {
    ec.message.addError("Username is required for BASIC auth.")
}

if (!ec.message.hasError()) {
    def existingAuth = ec.entity.find("darpan.reconciliation.NsAuthConfig")
        .condition("nsAuthConfigId", nsAuthConfigIdValue)
        .useCache(false)
        .one()

    if (nsAuthTypeValue == "OAUTH2_M2M_JWT") {
        if (!nsTokenUrlValue) ec.message.addError("Token URL is required for OAUTH2_M2M_JWT auth.")
        if (!nsClientIdValue) ec.message.addError("Client ID is required for OAUTH2_M2M_JWT auth.")
        if (!nsCertIdValue) ec.message.addError("Cert ID is required for OAUTH2_M2M_JWT auth.")
        if (!nsPrivateKeyPemValue && !existingAuth?.privateKeyPem) {
            ec.message.addError("Private Key PEM is required for OAUTH2_M2M_JWT auth.")
        }
    }

    if (nsAuthTypeValue == "BEARER" && !nsApiTokenValue && !nsPasswordValue && !existingAuth?.apiToken && !existingAuth?.password) {
        ec.message.addMessage("Provide API Token (or password fallback) for BEARER auth.")
    }

    if (!ec.message.hasError()) {
        Map authMap = [
            nsAuthConfigId: nsAuthConfigIdValue,
            description: nsAuthDescriptionValue,
            authType: nsAuthTypeValue,
            username: nsUsernameValue,
            tokenUrl: nsTokenUrlValue,
            clientId: nsClientIdValue,
            certId: nsCertIdValue,
            scope: nsScopeValue,
            isActive: nsIsActiveValue,
        ]

        if (nsPasswordValue) authMap.password = nsPasswordValue
        else if (existingAuth?.password) authMap.password = existingAuth.password

        if (nsApiTokenValue) authMap.apiToken = nsApiTokenValue
        else if (existingAuth?.apiToken) authMap.apiToken = existingAuth.apiToken

        if (nsPrivateKeyPemValue) authMap.privateKeyPem = nsPrivateKeyPemValue
        else if (existingAuth?.privateKeyPem) authMap.privateKeyPem = existingAuth.privateKeyPem

        ec.service.sync().name("store#darpan.reconciliation.NsAuthConfig").parameters(authMap).call()

        savedAuthConfig = [
            nsAuthConfigId: nsAuthConfigIdValue,
            description: nsAuthDescriptionValue,
            authType: nsAuthTypeValue,
            username: nsUsernameValue,
            tokenUrl: nsTokenUrlValue,
            clientId: nsClientIdValue,
            certId: nsCertIdValue,
            scope: nsScopeValue,
            isActive: nsIsActiveValue,
            hasPassword: !!FacadeSupport.normalize(authMap.password),
            hasApiToken: !!FacadeSupport.normalize(authMap.apiToken),
            hasPrivateKeyPem: !!FacadeSupport.normalize(authMap.privateKeyPem),
        ]

        if (!ec.message.hasError()) ec.message.addMessage("Saved NS Auth config ${nsAuthConfigIdValue}.")
    }
}

Map envelope = FacadeSupport.envelope(ec)
ok = envelope.ok
messages = envelope.messages
errors = envelope.errors
