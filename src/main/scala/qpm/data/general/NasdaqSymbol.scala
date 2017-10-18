package qpm.data.general

case class NasdaqSymbol(symbol: String) extends AnyVal{
  override def toString: String = symbol
  def toGoogleFinanceSymbol: GoogleFinanceSymbol =
    GoogleFinanceSymbol(symbol.replaceAll("\\^", "-"))
}