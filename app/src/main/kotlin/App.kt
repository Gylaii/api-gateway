package com.example

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.application
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.auth.Authentication
import io.ktor.server.auth.authenticate
import io.ktor.server.auth.jwt.JWTPrincipal
import io.ktor.server.auth.jwt.jwt
import io.ktor.server.auth.principal
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.callloging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.routing
import io.lettuce.core.RedisClient
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.pubsub.RedisPubSubAdapter
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Serializable
data class RegisterRequest(val email: String, val password: String, val name: String)

@Serializable
data class LoginRequest(val email: String, val password: String)

@Serializable
data class AuthResponse(val token: String, val message: String, val correlationId: String)

@Serializable
data class RequestMessage(val type: String, val correlationId: String, val payload: String)

@Serializable
data class ResponseMessage(val correlationId: String, val payload: String)

@Serializable
data class UserProfile(
    val id: String,
    val email: String,
    val name: String,
    val height: Int? = null,
    val weight: Int? = null,
    val goal: String? = null,
    val activityLevel: String? = null,
    val createdAt: String
)

@Serializable
data class UpdateInfoRequest(
    val name: String
)

@Serializable
data class UpdateMetricsRequest(
    val height: Int? = null,
    val weight: Int? = null,
    val goal: String? = null,
    val activityLevel: String? = null
)

@Serializable
data class UserInfo(
    val id: String,
    val email: String,
    val name: String,
    val createdAt: String
)

@Serializable
data class UserMetrics(
    val height: Int? = null,
    val weight: Int? = null,
    val goal: String? = null,
    val activityLevel: String? = null
)

@Serializable
data class HistoryQueryParams(
    val from: String? = null,
    val to: String? = null,
    val field: String? = null
)

@Serializable
data class HistoryRecord(
    val field: String,
    val oldValue: Int?,
    val newValue: Int?,
    val changedAt: String
)

@Serializable
data class SearchMeal(
    @SerialName("correlation_id")
    val correlationId: String,
    @SerialName("search_term")
    val searchTerm: String,
    @SerialName("page")
    val page: Int,
    @SerialName("page_size")
    val pageSize: Int,
)

suspend inline fun <reified T> ApplicationCall.updateThroughQueue(
    type: String,
    userId: String,
    body: T,
    commands: RedisCommands<String, String>,
    responseChannel: Channel<ResponseMessage>
): Any {
    val correlationId = UUID.randomUUID().toString()
    val payload = "$userId;${Json.encodeToString(body)}"
    commands.lpush(
        "user-service:request-queue",
        Json.encodeToString(RequestMessage(type, correlationId, payload))
    )

    return try {
        withTimeout(5_000) {
            while (true) {
                val resp = responseChannel.receive()
                if (resp.correlationId == correlationId) {
                    val profile = Json.decodeFromString<UserProfile?>(resp.payload)
                    return@withTimeout profile ?: HttpStatusCode.NotFound
                }
            }
        }
    } catch (_: TimeoutCancellationException) {
        HttpStatusCode.InternalServerError
    }
}

suspend inline fun <reified T> ApplicationCall.requestOne(
    type: String,
    userId: String,
    commands: RedisCommands<String, String>,
    responseChannel: Channel<ResponseMessage>
): Any {
    val correlationId = UUID.randomUUID().toString()
    commands.lpush(
        "user-service:request-queue",
        Json.encodeToString(RequestMessage(type, correlationId, userId))
    )

    return try {
        withTimeout(5_000) {
            while (true) {
                val resp = responseChannel.receive()
                if (resp.correlationId == correlationId) {
                    val obj = Json.decodeFromString<T?>(resp.payload)
                    return@withTimeout obj ?: HttpStatusCode.NotFound
                }
            }
        }
    } catch (_: TimeoutCancellationException) {
        HttpStatusCode.InternalServerError
    }
}

suspend fun ApplicationCall.respondSmart(result: Any) {
    when (result) {
        is HttpStatusCode -> respond(result)
        else -> respond(HttpStatusCode.OK, result)
    }
}


fun main() {
    val logger = LoggerFactory.getLogger("APIGateway")

    val redisClient =
        RedisClient.create("redis://${System.getenv("KEYDB_HOST") ?: "localhost"}:${System.getenv("KEYDB_PORT") ?: "6379"}")
    val connection: StatefulRedisConnection<String, String> = redisClient.connect()
    val commands: RedisCommands<String, String> = connection.sync()

    val pubSubConnection: StatefulRedisPubSubConnection<String, String> = redisClient.connectPubSub()
    val responseChannel = Channel<ResponseMessage>()

    pubSubConnection.async().subscribe("api-gateway:response-channel")
    pubSubConnection.async().subscribe("nutrition-service:response-channel")
    val pendingResponses = ConcurrentHashMap<String, CompletableDeferred<ResponseMessage>>()
    pubSubConnection.addListener(object : RedisPubSubAdapter<String, String>() {
        override fun message(channel: String?, message: String?) {
            if (message != null) {
                when (channel) {
                    "api-gateway:response-channel" -> {
                        logger.debug("Получено сообщение в Pub/Sub канале: $message")
                        val responseMsg: ResponseMessage = Json.decodeFromString(message)
                        GlobalScope.launch {
                            responseChannel.send(responseMsg)
                        }
                    }

                    "nutrition-service:response-channel" -> {
                        val responseMsg: ResponseMessage = Json.decodeFromString(message)
                        pendingResponses[responseMsg.correlationId]?.complete(responseMsg)
                    }
                }
            }
        }
    })

    val jwtSecret = "secret"
    val jwtIssuer = "issuer"
    val jwtRealm = "realm"

    embeddedServer(Netty, port = 8080) {
        install(ContentNegotiation) {
            json()
        }
        install(CallLogging)

        install(Authentication) {
            jwt("auth-jwt") {
                realm = jwtRealm
                verifier(
                    JWT
                        .require(Algorithm.HMAC256(jwtSecret))
                        .withIssuer(jwtIssuer)
                        .build()
                )
                validate { credential ->
                    val emailClaim = credential.payload.getClaim("email").asString()
                    if (emailClaim.isNotEmpty()) {
                        logger.info("JWT токен валиден, email: $emailClaim")
                        JWTPrincipal(credential.payload)
                    } else null
                }
            }
        }

        routing {
            post("/api/auth/register") {
                application.environment.log.info("Получен запрос на регистрацию")
                logger.debug("Начало обработки запроса /api/auth/register")

                val registerRequest = call.receive<RegisterRequest>()
                val correlationId = UUID.randomUUID().toString()
                val payload = Json.encodeToString(registerRequest)

                val requestMsg = RequestMessage("register", correlationId, payload)
                val requestJson = Json.encodeToString(requestMsg)
                logger.info("Отправляем register запрос в user-service: correlationId=$correlationId")

                commands.lpush("user-service:request-queue", requestJson)

                var authResponse: AuthResponse? = null
                try {
                    withTimeout(5000L) {
                        while (true) {
                            val response: ResponseMessage = responseChannel.receive()
                            if (response.correlationId == correlationId) {
                                logger.debug("Получен ответ из канала для correlationId=$correlationId")
                                val payloadResponse: AuthResponse = Json.decodeFromString(response.payload)
                                authResponse = payloadResponse.copy(correlationId = correlationId)
                                break
                            }
                        }
                    }
                    logger.info("Регистрация успешно завершена: $authResponse")
                    call.respond(HttpStatusCode.OK, authResponse!!)
                } catch (e: TimeoutCancellationException) {
                    application.environment.log.error(
                        "Таймаут ожидания ответа для регистрации, correlationId=$correlationId",
                        e
                    )
                    logger.warn("Не удалось получить ответ за 5 сек, отправляем 500")
                    call.respond(HttpStatusCode.InternalServerError, "Timeout waiting for response")
                }
            }

            post("/api/auth/login") {
                application.environment.log.info("Получен запрос на авторизацию")
                logger.debug("Начало обработки запроса /api/auth/login")

                val loginRequest = call.receive<LoginRequest>()
                val correlationId = UUID.randomUUID().toString()
                val payload = Json.encodeToString(loginRequest)

                val requestMsg = RequestMessage("login", correlationId, payload)
                val requestJson = Json.encodeToString(requestMsg)
                logger.info("Отправляем login запрос в user-service: correlationId=$correlationId")

                commands.lpush("user-service:request-queue", requestJson)

                var authResponse: AuthResponse? = null
                try {
                    withTimeout(5000L) {
                        while (true) {
                            val response: ResponseMessage = responseChannel.receive()
                            if (response.correlationId == correlationId) {
                                logger.debug("Получен ответ из канала для correlationId=$correlationId")
                                val payloadResponse: AuthResponse = Json.decodeFromString(response.payload)
                                authResponse = payloadResponse.copy(correlationId = correlationId)
                                break
                            }
                        }
                    }
                    logger.info("Авторизация успешно завершена: $authResponse")
                    call.respond(HttpStatusCode.OK, authResponse!!)
                } catch (e: TimeoutCancellationException) {
                    application.environment.log.error(
                        "Таймаут ожидания ответа для логина, correlationId=$correlationId",
                        e
                    )
                    logger.warn("Не удалось получить ответ за 5 сек, отправляем 500")
                    call.respond(HttpStatusCode.InternalServerError, "Timeout waiting for response")
                }
            }

            authenticate("auth-jwt") {
                get("/api/protected") {
                    logger.debug("Вход в защищённый эндпоинт /api/protected")
                    val principal = call.principal<JWTPrincipal>()
                    val email = principal?.payload?.getClaim("email")?.asString() ?: "unknown"
                    logger.info("Пользователь $email получает доступ к /api/protected")
                    call.respond(HttpStatusCode.OK, "Доступ разрешён для пользователя: $email")
                }
                get("/api/user/profile/info") {
                    val uid = call.principal<JWTPrincipal>()!!.payload.getClaim("userId").asString()
                    val res = call.requestOne<UserInfo>(
                        "get-profile-info", uid, commands, responseChannel
                    )
                    call.respondSmart(res)
                }

                get("/api/user/profile/metrics") {
                    val uid = call.principal<JWTPrincipal>()!!.payload.getClaim("userId").asString()
                    val res = call.requestOne<UserMetrics>(
                        "get-profile-metrics", uid, commands, responseChannel
                    )
                    call.respondSmart(res)
                }

                put("/api/user/profile/info") {
                    val uid = call.principal<JWTPrincipal>()!!.payload.getClaim("userId").asString()
                    val body = call.receive<UpdateInfoRequest>()
                    val res = call.updateThroughQueue(
                        "update-profile-info", uid, body, commands, responseChannel
                    )
                    call.respondSmart(res)
                }

                put("/api/user/profile/metrics") {
                    val uid = call.principal<JWTPrincipal>()!!.payload.getClaim("userId").asString()
                    val body = call.receive<UpdateMetricsRequest>()
                    val res = call.updateThroughQueue(
                        "update-profile-metrics", uid, body, commands, responseChannel
                    )
                    call.respondSmart(res)
                }

                get("/api/user/profile/history") {
                    val uid = call.principal<JWTPrincipal>()!!.payload.getClaim("userId").asString()
                    val from = call.request.queryParameters["from"]
                    val to = call.request.queryParameters["to"]
                    val field = call.request.queryParameters["field"]

                    val payload = HistoryQueryParams(from, to, field)
                    val combined = "$uid;${Json.encodeToString(payload)}"
                    val res =
                        call.requestOne<List<HistoryRecord>>("get-metrics-history", combined, commands, responseChannel)
                    call.respondSmart(res)
                }

                get("/api/nutrition/search-meal") {
                    try {
                        val page = call.request.queryParameters["page"]?.toInt() ?: 0
                        val pageSize = call.request.queryParameters["page_size"]?.toInt() ?: 20
                        val searchTerm = call.request.queryParameters["search_term"]
                            ?: return@get call.respond(HttpStatusCode.BadRequest)

                        val requestId = UUID.randomUUID().toString()

                        val deferred = CompletableDeferred<ResponseMessage>()
                        pendingResponses[requestId] = deferred

                        val request = SearchMeal(
                            correlationId = requestId,
                            searchTerm = searchTerm,
                            page = page,
                            pageSize = pageSize
                        )

                        commands.publish(
                            "nutrition-service:request-channel",
                            Json.encodeToString(request)
                        )

                        try {
                            val response = withTimeout(5_000) {
                                deferred.await()
                            }
                            call.respond(HttpStatusCode.OK, response.payload)
                        } catch (e: TimeoutCancellationException) {
                            call.respond(HttpStatusCode.GatewayTimeout, "Timeout waiting for response")
                        } finally {
                            pendingResponses.remove(requestId)
                        }
                    } catch (e: NumberFormatException) {
                        call.respond(HttpStatusCode.BadRequest)
                    }
                }
            }
        }
    }.start(wait = true)

    logger.info("API Gateway запущен на порту 8080")
}
