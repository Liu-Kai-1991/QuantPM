package qpm.data.general

case class GoogleFinanceSymbol(symbol: String) extends AnyVal{
  override def toString: String = symbol
}