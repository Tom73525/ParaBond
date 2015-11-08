package parabond.entry

/**
 * @author Ron.Coleman
 */
/**
 * This class implements a simple bond.
 * @param id Bond id
 * @param coupon Coupon cash flow paid at frequency
 * @param freq Frequency per year, e.g., 1 = yearly, 12 = monthly, etc.
 * @param tenor Tenor in years
 * @param maturity Amount due at maturity
 */
class SimpleBond(val id : Int, val coupon : Double, val freq : Int, val tenor : Double, val maturity : Double, value: Double) {
  def this(id: Int, coupon : Double, freq : Int, tenor : Double,  maturity : Double) = this(id,coupon,freq,tenor,maturity, 0.0)
  def this() = this(-1,-1,-1,-1,-1,-1)
  
  /** Converts bond to a string */
  override def toString = fmt(id,"%05d") + "," + fmt(coupon,"%5.2f") + "," + fmt(freq,"%3d") + "," + fmt(tenor,"%4.1f") + "," + fmt(maturity,"%6.1f")

  /**
   * Formats a string.
   * @param value Value
   * @param specifier Format specifier
   */
  // See http://stackoverflow.com/questions/1350566/number-formatting-in-scala
  def fmt(value: Any, specifier : String): String = value match {
    case d: Double => specifier.format(d)
    
    case i: Int => specifier.format(i)
    
    case _ => throw new IllegalArgumentException
  }
}

object SimpleBond {
  def apply(id : Int, coupon : Double, freq : Int, tenor : Double, maturity : Double) = new SimpleBond(id,coupon,freq,tenor,maturity,0.0)
  def apply() = new SimpleBond
}