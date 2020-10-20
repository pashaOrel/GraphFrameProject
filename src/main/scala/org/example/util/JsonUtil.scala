package org.example.util


import com.typesafe.scalalogging.LazyLogging

import scala.io.Source
import io.circe.{Decoder, Encoder, parser}
/**
 * Общие методы для работы с circe
 */
object JsonUtil extends LazyLogging {

  /**
   * Декодирование объекта из строки
   *
   * @param jsonStr строка с json
   * @param decoder для стандартных типов достачно сделать import io.circe.generic.auto._
   * @tparam T тип объекта
   * @return Либо ошибку декодирования, либо объект
   */
  def fromJson[T](jsonStr: String)(implicit decoder: Decoder[T]): Either[io.circe.Error, T] = {
    val result = parser.parse(jsonStr).right.flatMap(json => json.as[T])
    if (result.isLeft) {
      logger.error(s"Could not deserialize json $jsonStr to object")
    }
    result
  }

  /**
   *
   * @param obj     объект
   * @param encoder для стандартных типов достачно сделать import io.circe.generic.auto._
   * @tparam T тип объекта
   * @return строку в формате json
   */
  def asJson[T](obj: T)(implicit encoder: Encoder[T]): String = {
    import io.circe.syntax._
    try {
      obj.asJson.noSpaces
    } catch {
      case ex: Exception =>
        logger.error(s"Could not serialize object $obj to json", ex)
        throw ex
    }
  }

  /*
    * Для загрузки из файла в тестах
    */
  def fromJsonFileUnsafe[T](fileName: String)(implicit decoder: Decoder[T]): T = {
    val inputStream = getClass.getResourceAsStream(fileName)
    if (inputStream == null) {
      throw new IllegalArgumentException(s"File not found: $fileName")
    }
    val jsonString = Source.fromInputStream(inputStream).mkString
    fromJson(jsonString) match {
      case Right(result) => result
      case Left(error) => throw error
    }
  }

}
