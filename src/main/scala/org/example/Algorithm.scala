package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.example.template.{Template, TemplateLinkParameter}
import org.example.util.DataFrameOperator
import org.graphframes.GraphFrame

object Algorithm extends App {


  val spark = getSpark
  spark.sql("CREATE SCHEMA IF NOT EXISTS tmp_tos")
  spark.sql("USE tmp_tos")


  transformation()


  def transformation(): Unit = {

    val template = returnJsonFileAsObject("C://Users/porlov/Desktop/hive/new_data/schema 427.json")
    val filterLinks = getFilterLinksCondition("coop", template)
    val (linkSql, participantSql) = DataFrameOperator.createSql(
      filterLinks,
      s"tmp_tos.${Constants.PARTICIPANT_TABLE_NAME}",
      s"tmp_tos.${Constants.LINK_TABLE_NAME}"
    )
    val linkDf = DataFrameOperator.normalizeBigDecimal(spark.sql(linkSql))
    val participantDf = DataFrameOperator.normalizeBigDecimal(spark.sql(participantSql))

    val vertices = getVertices(participantDf)
    val edges = getEdges(linkDf)
    val graph = GraphFrame(vertices, edges)

    val chain3 = graph
      .find("(odn)-[odn_linked_to_transit]->(transit); (transit)-[transit_linked_to_vgp]->(vgp)")
      .filter("odn.role = 'OD'")
      .filter("transit.role = 'TRANSIT'")
      .filter("vgp.role = 'VGP'")

    val chain4 = graph
      .find("" +
        "(odn)-[odn_linked_to_transit1]->(transit1); " +
        "(transit1)-[transit1_linked_to_transit2]->(transit2); " +
        "(transit2)-[transit2_linked_to_vgd]->(vgd)")
      .filter("odn.role = 'OD'")
      .filter("transit1.role = 'TRANSIT'")
      .filter("transit2.role = 'TRANSIT'")
      .filter("vgd.role = 'VGP'")


    def convertRowToMap[T](row: Row): Map[String, T] = {
      row.schema.fieldNames.filter(field => !row.isNullAt(row.fieldIndex(field))).map(
        field => field -> row.getAs[T](field)
      ).toMap
    }
    def convertRowToParticipant(row: Row): Participant = {
      Participant(
        row.getAs[String]("id"),
        row.getAs[String]("seller_mis_total"),
        row.getAs[String]("decl_is_null"),
        row.getAs[String]("decl_is_zero"),
        row.getAs[String]("is_real_np"),
        row.getAs[String]("sur"),
        row.getAs[String]("is_not_transiter"),
        row.getAs[String]("role")
      )
    }

    def convertRowToLink(row: Row): Link = {
      Link(
        row.getAs[String]("dst"),
        row.getAs[String]("src"),
        row.getAs[String]("tpr_mis_total"),
        row.getAs[String]("tpr_vat_buyer_from_seller"),
        row.getAs[String]("tpr_vat_8_total")
      )
    }

    def rowToSchema: Row => Schema = (row: Row) => {
      val map_temp = convertRowToMap[Row](row)

      val (participants, links) = map_temp.partition { case (columnName, _) => !columnName.contains("_linked_to_") }

      Schema(
        participants.values.map(convertRowToParticipant).toList,
        links.values.map(convertRowToLink).toList
      )
    }


    import spark.implicits._
    val example_chain3 = chain3.map(rowToSchema)
    val example_chain4 = chain3.map(rowToSchema)

    example_chain3.show(2, false)
    example_chain4.show(2, false)
    example_chain3.printSchema()

    example_chain3.write.mode(SaveMode.Append).saveAsTable("result_table2")
    example_chain4.write.mode(SaveMode.Append).saveAsTable("result_table2")

  }


  def getEdges(df: DataFrame): DataFrame = {
    df.
      select(
        concat_ws(
          "->",
          col("coop.inn_1"),
          col("coop.kpp_effective_1"),
          col("year"),
          col("quarter"))
          .as("dst"),
        concat_ws(
          "->",
          col("coop.inn_2"),
          col("coop.kpp_effective_2"),
          col("year"),
          col("quarter"))
          .as("src"),
        col("coop.tpr_mis_total").as("tpr_mis_total"),
        col("coop.tpr_vat_buyer_from_seller").as("tpr_vat_buyer_from_seller"),
        col("coop.tpr_vat_8_total").as("tpr_vat_8_total"),

      )
  }

  def getVertices(df: DataFrame): DataFrame = {

    df.
      select(
        concat_ws(
          "->",
          col("inn"),
          col("kpp_effective"),
          col("year"),
          col("quarter")
        ).as("id"),
        col("seller_mis_total"),
        col("decl_is_null"),
        col("decl_is_zero"),
        col("is_real_np"),
        col("sur"),
        col("is_not_transiter")
      )
      .withColumn("role",
        when(
          (col("decl_is_zero") === "1" ||
            col("decl_is_null") === "1" ||
            col("sur") === "1" || col("sur") === "4") &&
            (col("seller_mis_total") > "3000000"), "OD"
        )
          .otherwise(
            when(
              (col("is_real_np") === null || col("is_real_np") =!= "1") &&
                col("is_not_transiter") =!= "1", "TRANSIT"
            )
              .otherwise(
                when(col("is_real_np") === "1", "VGP")
                  .otherwise("UNDEF")
              )
          )

      )
  }

  def getFilterLinksCondition(alias: String, template: Template): String = {
    def and(params: List[TemplateLinkParameter]): Option[String] = {
      val conditions = for (cond <- params if cond.value.isDefined; value = cond.value.get) yield cond.brief match {
        case "COOPERATION_gap_sum_seller_byer" => s"$alias.tpr_mis_total > $value"
        case "COOPERATION_total_sum_NDS" => s"$alias.tpr_vat_buyer_from_seller > $value"
        case "COOPERATION_NDS_deduction_seller" => s"$alias.tpr_vat_8_total > $value"
        case _ => "1=1"
      }
      if (conditions.nonEmpty) Some(conditions.mkString(" AND ")) else None
    }

    val orConditions: List[String] = for {
      link <- template.links if link.linkType == "COOPERATION"
      linkCondition <- and(link.parameters)
    } yield linkCondition

    if (orConditions.nonEmpty) s" AND (${orConditions.mkString(" OR ")})" else ""
  }

  def returnJsonFileAsObject(path: String): Template = {
    val testTxtSource = scala.io.Source.fromFile(path)
    val str = testTxtSource.mkString
    testTxtSource.close()

    val leftOrRight = Template.fromJson(str)

    leftOrRight match {
      case Left(error) => throw new IllegalArgumentException("Ошибка чтения json шаблона: " + error.getMessage)
      case Right(template) => template
    }

  }


  def getSpark: SparkSession = {

    val spconf = new SparkConf()
      .setAppName("GraphFrameProject")
      .set("hive.execution.engine", "spark")

    SparkSession
      .builder()
      .master("local")
      .config(spconf)
      .enableHiveSupport()
      .getOrCreate()
  }


}
