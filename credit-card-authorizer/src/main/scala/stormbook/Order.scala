package stormbook

import play.api.libs.json.{JsPath, Reads}
import play.api.libs.functional.syntax._

case class Order(id: Long,
                 customerId: Long,
                 creditCardNumber: Long,
                 creditCardExpiration: String,
                 creditCardCode: Int,
                 chargeAmount: Double) extends Serializable {
}

object Order {
  implicit val orderReads: Reads[Order] = (
    (JsPath \ "id").read[Long] and
      (JsPath \ "customerId").read[Long] and
      (JsPath \ "creditCardNumber").read[Long] and
      (JsPath \ "creditCardExpiration").read[String] and
      (JsPath \ "creditCardCode").read[Int] and
      (JsPath \ "chargeAmount").read[Double]
    )(Order.apply _)

}
