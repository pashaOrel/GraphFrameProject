package org.example

sealed abstract class ParentModel

case class Participant(
               id: String,
               seller_mis_total: String,
               decl_is_null: String,
               decl_is_zero: String,
               is_real_np: String,
               sur: String,
               is_not_transiter: String,
               role: String,
               ) extends ParentModel
case class Link(
               dst: String,
               src: String,
               tpr_mis_total: String,
               tpr_vat_buyer_from_seller: String,
               tpr_vat_8_total: String
               ) extends ParentModel

case class Schema(
                 participants: List[Participant],
                 links: List[Link]
                 )