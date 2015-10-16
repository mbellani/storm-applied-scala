package stormbook

class AuthService {
  def authorize(order: Order): AuthResponse.AuthResponse = {
    AuthResponse.Success(order)
  }
}

object AuthResponse {

  trait AuthResponse {
    def order: Order
  }

  case class Success(order: Order) extends AuthResponse

  case class Failure(order: Order) extends AuthResponse

}


class AuthResponseHandler(success: (Order) => Unit, failure: (Order) => Unit) {
  def handle(authResponse: AuthResponse.AuthResponse): Unit = {
    authResponse match {
      case authResponse: AuthResponse.Success => success(authResponse.order)
      case _ => failure(authResponse.order)
    }
  }
}
