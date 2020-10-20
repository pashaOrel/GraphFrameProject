package org.example.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType}

object DataFrameOperator {

  def createSql(filterLinks: String, participants: String, links: String): (String, String) = {

    val whereCond_link_noJoin =
      s"""coop.fid1 is not null AND coop.fid1 != 0
         |AND coop.fid2 is not null AND coop.fid2 != 0
         |AND coop.rel_invoice_flag = 1 AND coop.is_real_inn_2 == true
         |AND coop.year IS NOT NULL AND coop.year != 0 AND coop.quarter IS NOT NULL AND coop.quarter != 0 ${filterLinks}""".stripMargin

    val sql_link_noJoin =
      s"""SELECT
         | coop.inn_1, coop.inn_2, coop.kpp_effective_1, coop.kpp_effective_2, CAST(coop.year AS int), cast(coop.quarter AS int),
         | coop.tpr_mis_total, coop.tpr_vat_buyer_from_seller, coop.tpr_vat_8_total
         | FROM $links coop
         | WHERE $whereCond_link_noJoin""".stripMargin

    val whereParticipantsCond =
      s""" p.vertex_num IS NOT NULL
         | AND p.inn IS NOT NULL
         | AND p.kpp_effective IS NOT NULL
         | AND p.year IS NOT NULL AND p.year != 0
         | AND p.quarter IS NOT NULL AND p.quarter != 0
         | AND (lower(p.`_tech_action_flg`) <> 'd' OR p.`_tech_action_flg` IS NULL)""".stripMargin

    val sqlParticipants =
      s"""SELECT
         |   CAST(year AS int), CAST(quarter AS int), inn, kpp_effective,
         |   seller_mis_total, decl_is_null, decl_is_zero, is_real_np, sur, is_not_transiter
         | FROM $participants p WHERE $whereParticipantsCond""".stripMargin

    (sql_link_noJoin, sqlParticipants)
  }




  def normalizeBigDecimal(dataFrame: DataFrame): DataFrame = {
    val schemaDF = dataFrame.schema
    var df = dataFrame
    schemaDF.foreach { field =>
      field.dataType match {
        //по-умолчанию целые приводим к Int. Там где явно требуется Long - приводим в Long прямо в SQL запросе
        case t: DecimalType if t.scale == 0 =>
          df = df.withColumn(field.name, col(field.name).cast(IntegerType))
        // все Decimal типы приводим к 38,18 иначе в scala мы получаем ошибку
        case t: DecimalType if t.scale > 0 && t != DecimalType(38, 18) =>
          df = df.withColumn(field.name, col(field.name).cast(DecimalType(38, 18)))
        // double приводим к Decimal(38,18)
        case t: DoubleType =>
          df = df.withColumn(field.name, col(field.name).cast(DecimalType(38, 18)))
        case _ => //do nothing
      }
    }
    df
  }

}
