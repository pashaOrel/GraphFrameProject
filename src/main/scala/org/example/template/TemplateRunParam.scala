package org.example.template

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

case class TemplateRunParam (id: Long,
                             template_version_id: Long,
                             template_name: String,
                             template_json: String,
                             is_active: Boolean,
                             run_type: String,
                             run_method: String,
                             scheduled_date: Option[java.sql.Date]) {
  def getTemplate: Template = {
    Template.fromJson(template_json) match {
      case Left(error) => throw new IllegalArgumentException("Ошибка чтения json шаблона: " + error.getMessage)
      case Right(template) => template
    }
  }
}
/**
 * Reads template run param from hive table
 */
object TemplateRunParam {

  private implicit val encoder: Encoder[TemplateRunParam] = Encoders.product[TemplateRunParam]

  //  private val table = "dl_tos.template_run_param"
  val columns = List("CAST(id as bigint)", "CAST(template_version_id as bigint)", "template_name", "template_json", "run_type", "run_method", "scheduled_date", "is_active")

  /**
   *  параметры template_run_param - имя и схема таблицы должны быть заранее определены в hive (БУМ)
   * @param spark
   * @return списко шаблонов для запуска (в автоматическом режиме)
   */
  def loadScheduledTemplateRunParams(implicit spark: SparkSession): Array[TemplateRunParam] = {
    spark.sql(s"SELECT ${columns.mkString(",")} " +
      s" FROM $${template_run_param_table} WHERE run_method = 'SCHEDULED' AND is_active = true").as[TemplateRunParam].collect()
  }

  /**
   * параметры template_run_param, templateRunParamId - должны быть заранее определены в hive (БУМ)
   * @param spark spark session
   * @param templateRunParamId manual run id
   * @return список шаблонов для запуска (в ручном режиме)
   */
  def loadManualTemplateRunParam(templateRunParamId: Long)(implicit spark: SparkSession): Array[TemplateRunParam] = {
    spark.sql(s"SELECT ${columns.mkString(",")} " +
      s" FROM $${template_run_param_table} WHERE run_method = 'MANUAL' AND id = ${templateRunParamId}").as[TemplateRunParam].collect()
  }
}