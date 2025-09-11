using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// The compression used in the Bloom filter.
/// </summary>
public class Uncompressed {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static Uncompressed Read(ThriftCompactProtocolReader proto) {
        var r = new Uncompressed();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                default:
                    proto.SkipField(compactType);
                    break;
            }
        }
        proto.StructEnd();
        return r;
    }
}