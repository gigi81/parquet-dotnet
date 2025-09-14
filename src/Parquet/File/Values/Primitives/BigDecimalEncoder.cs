using System;
using System.Linq;
using System.Numerics;

namespace Parquet.File.Values.Primitives {
    /// <summary>
    /// BigDecimal encoder
    /// </summary>
    public class BigDecimalEncoder {
        private readonly int _precision;
        private readonly bool _isBigEndian;
        private readonly BigInteger _scaleMultiplier;
        private readonly int _bufferSize;

        /// <summary>
        /// Creates a BigDecimal encoder
        /// </summary>
        /// <param name="precision"></param>
        /// <param name="scale"></param>
        /// <param name="isBigEndian"></param>
        public BigDecimalEncoder(int precision, int scale, bool isBigEndian = true) {
            _precision = precision;
            _isBigEndian = isBigEndian;
            _scaleMultiplier = BigInteger.Pow(10, scale);
            _bufferSize = GetBufferSize(precision);
        }
        
        /// <summary>
        /// Buffer size in bytes needed to hold the encoded decimal value
        /// </summary>
        public int BufferSize => _bufferSize;
        
        /// <summary>
        /// Gets a byte array from a decimal value
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public byte[] GetBytes(decimal value) {
            /*
             * Java: https://docs.oracle.com/javase/7/docs/api/java/math/BigInteger.html#toByteArray()
             *
             * Returns a byte array containing the two's-complement representation of this BigInteger.
             * The byte array will be in big-endian byte-order: the most significant byte is in the zeroth element.
             * The array will contain the minimum number of bytes required to represent this BigInteger,
             * including at least one sign bit, which is (ceil((this.bitLength() + 1)/8)).
             * (This representation is compatible with the (byte[]) constructor.)
             *
             * C#:   https://msdn.microsoft.com/en-us/library/system.numerics.biginteger.tobytearray(v=vs.110).aspx
             *
             *
             *  value | C# | Java
             *
             * -1 | [1111 1111] | [1111 1111] - no difference, so maybe buffer size?
             *
             */

            var bscaled = new BigInteger(value);
            decimal scaled = value - (decimal)bscaled;
            decimal unscaled = scaled * (decimal)_scaleMultiplier;
            var unscaledValue = (bscaled * _scaleMultiplier) + new BigInteger(unscaled);

            byte[] result = AllocateResult();
            byte[] data = unscaledValue.ToByteArray();
            if(data.Length > result.Length)
                throw new NotSupportedException($"decimal data buffer is {data.Length} but result must fit into {result.Length} bytes");

            Array.Copy(data, result, data.Length);

            //if value is negative fill the remaining bytes with [1111 1111] i.e. negative flag bit (0xFF)
            if(unscaledValue.Sign == -1) {
                for(int i = data.Length; i < result.Length; i++) {
                    result[i] = 0xFF;
                }
            }

            result = result.Reverse().ToArray();
            return result;
        }
        
        private byte[] AllocateResult() {
            int size = GetBufferSize(_precision);
            return new byte[size];
        }

        /// <summary>
        /// Gets buffer size enough to be able to hold the decimal number of a specific precision
        /// </summary>
        /// <param name="precision">Precision value</param>
        /// <returns>Length in bytes</returns>
        public static int GetBufferSize(int precision) {
            //according to impala source: http://impala.io/doc/html/parquet-common_8h_source.html

            switch(precision) {
                case 1:
                case 2:
                    return 1;
                case 3:
                case 4:
                    return 2;
                case 5:
                case 6:
                    return 3;
                case 7:
                case 8:
                case 9:
                    return 4;
                case 10:
                case 11:
                    return 5;
                case 12:
                case 13:
                case 14:
                    return 6;
                case 15:
                case 16:
                    return 7;
                case 17:
                case 18:
                    return 8;
                case 19:
                case 20:
                case 21:
                    return 9;
                case 22:
                case 23:
                    return 10;
                case 24:
                case 25:
                case 26:
                    return 11;
                case 27:
                case 28:
                    return 12;
                case 29:
                case 30:
                case 31:
                    return 13;
                case 32:
                case 33:
                    return 14;
                case 34:
                case 35:
                    return 15;
                case 36:
                case 37:
                case 38:
                    return 16;
                default:
                    return 16;
            }
        }
    }
}