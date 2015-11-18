/*
 * Copyright (c) Ron Coleman
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Scaly Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package parabond.entry

/**
 * This class implements a simple bond.
 * @param id Bond id
 * @param coupon Coupon cash flow paid at frequency
 * @param freq Frequency per year, e.g., 1 = yearly, 12 = monthly, etc.
 * @param tenor Tenor in years
 * @param maturity Amount due at maturity
 * @param value Value of the bond
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