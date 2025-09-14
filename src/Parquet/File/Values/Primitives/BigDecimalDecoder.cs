using System;
using System.Numerics;
using Parquet.Meta;

namespace Parquet.File.Values.Primitives {
    /// <summary>
    /// BigDecimal decoder
    /// </summary>
    public class BigDecimalDecoder {
        private readonly bool _isBigEndian;
        private readonly BigInteger _scaleMultiplier;
        private readonly decimal _scaleMultiplierDecimal;

        /// <summary>
        /// Creates a BigDecimal decoder
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="isBigEndian"></param>
        public BigDecimalDecoder(SchemaElement schema, bool isBigEndian = true) {
            _isBigEndian = isBigEndian;
            _scaleMultiplier = BigInteger.Pow(10, schema.Scale ?? 0);
            _scaleMultiplierDecimal = (decimal)_scaleMultiplier;
        }
        
        /// <summary>
        /// Decodes a byte array into a decimal
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public decimal Decode(ReadOnlySpan<byte> data) {
#if NETSTANDARD2_1_OR_GREATER || NET6_0_OR_GREATER
            var unscaledValue = new BigInteger(data, isUnsigned: false, !_isBigEndian);
#else
            byte[] array = data.ToArray();
            if(_isBigEndian) {
                Array.Reverse(array);
            }
            var unscaledValue = new BigInteger(array);
#endif
            
            var ipScaled = BigInteger.DivRem(unscaledValue, _scaleMultiplier, out BigInteger fpUnscaled);
            decimal decimalIpScaled = (decimal)ipScaled;
            decimal decimalFpScaled = (decimal)fpUnscaled / _scaleMultiplierDecimal;

            return decimalIpScaled + decimalFpScaled;
        }
    }
}