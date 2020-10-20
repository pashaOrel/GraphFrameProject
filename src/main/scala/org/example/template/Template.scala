package org.example.template

import org.example.util.JsonUtil
object Template {
  def asJson(template: Template): String = {
    import io.circe.generic.auto._
    JsonUtil.asJson[Template](template)
  }

  def fromJson(jsonStr: String): Either[io.circe.Error, Template] = {
    import io.circe.generic.auto._
    JsonUtil.fromJson[Template](jsonStr)
  }

}

case class TemplateLink(id: Long,
                        linkType: String,
                        linkTypeId: Long,
                        directed: Boolean,
                        p1: Long, p2: Long,
                        maxWeight: Int = 100,
                        parameters: List[TemplateLinkParameter] = List.empty)

case class TemplateLinkParameter(id: Long,
                                 linkId: Long,
                                 brief: String,
                                 parameterTypeId: Long,
                                 value: Option[String], //todo: pass parameter type
                                 mandatory: Boolean,
                                 weight: Int = 100 /* сейчас всегда 100 - от весов решили отказаться*/
                                )


case class TemplateParticipant(id: Long, min: Int, max: Int,
                               role: String,
                               roleId: Long,
                               maxWeight: Int = 100,
                               parameters: List[TemplateParticipantParameter] = List.empty,
                               regions: List[String] = List.empty,
                               businessRoles: List[String] = List.empty)

case class TemplateParticipantParameter(id: Long,
                                        participantId: Long,
                                        brief: String,
                                        parameterTypeId: Long,
                                        value: Option[String], //todo: pass parameter type
                                        mandatory: Boolean,
                                        weight: Int = 100 /* Сейчас всегда 100 - от весов решили отказаться*/
                                       )

case class Template(templateId: Long,
                    templateVersionId: Long,
                    name: String,
                    participants: List[TemplateParticipant] = List.empty,
                    links: List[TemplateLink] = List.empty,
                    parameters: Option[TemplateParameters] = None)


/**
 * Доп. параметры шаблона
 *
 * @param deviationPercentMin Допустимое уменьшение % отклонения кооперации (от -100 до +100)
 * @param deviationPercentMax Допустимое увеличение % отклонения кооперации (от -100 до +100)
 * @param percentMatch        общий процент совпадения с шаблоном (от 0 до 100) - сейчас всегда 0.
 */
case class TemplateParameters(
                               deviationPercentMin: Option[Int],
                               deviationPercentMax: Option[Int],
                               percentMatch: Option[Int] = Some(0) /* общий процент совпадения с шаблоном - сейчас отказались от этого параметра на UI */
                             )


