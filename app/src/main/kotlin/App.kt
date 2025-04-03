package com.example

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.callloging.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.serialization.kotlinx.json.*

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

import io.lettuce.core.RedisClient
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.pubsub.RedisPubSubAdapter
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import io.lettuce.core.api.StatefulRedisConnection

import org.slf4j.LoggerFactory
import java.util.UUID

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm

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

fun main() {
    val logger = LoggerFactory.getLogger("APIGateway")

    val redisClient = RedisClient.create("redis://localhost:6379")
    val connection: StatefulRedisConnection<String, String> = redisClient.connect()
    val commands: RedisCommands<String, String> = connection.sync()

    val pubSubConnection: StatefulRedisPubSubConnection<String, String> = redisClient.connectPubSub()
    val responseChannel = Channel<ResponseMessage>()

    pubSubConnection.async().subscribe("api-gateway:response-channel")
    pubSubConnection.addListener(object : RedisPubSubAdapter<String, String>() {
        override fun message(channel: String?, message: String?) {
            if (channel == "api-gateway:response-channel" && message != null) {
                logger.debug("Получено сообщение в Pub/Sub канале: $message")
                val responseMsg: ResponseMessage = Json.decodeFromString(message)
                GlobalScope.launch {
                    responseChannel.send(responseMsg)
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
                    application.environment.log.error("Таймаут ожидания ответа для регистрации, correlationId=$correlationId", e)
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
                    application.environment.log.error("Таймаут ожидания ответа для логина, correlationId=$correlationId", e)
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
            }
        }
    }.start(wait = true)

    logger.info("API Gateway запущен на порту 8080")
}
