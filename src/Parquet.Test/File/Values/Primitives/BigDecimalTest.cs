using Parquet.File.Values.Primitives;
using Xunit;

namespace Parquet.Test.File.Values.Primitives;

public class BigDecimalTest {
    [Fact]
    public void Valid_but_massive_bigdecimal() {
        var encoder = new BigDecimalEncoder( 38, 16);
        byte[] data = encoder.GetBytes(83086059037282.54m);

        //if exception is not thrown (overflow) we're OK
        //TODO: validate data
    }
}