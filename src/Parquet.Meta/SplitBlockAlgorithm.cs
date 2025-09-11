using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Block-based algorithm type annotation.
/// </summary>
public class SplitBlockAlgorithm {

    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.WriteEmptyStruct();
    }

    internal static SplitBlockAlgorithm Read(ThriftCompactProtocolReader proto) {
        var r = new SplitBlockAlgorithm();
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