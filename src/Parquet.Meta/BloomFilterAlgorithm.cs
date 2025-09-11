using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// The algorithm used in Bloom filter.
/// </summary>
public class BloomFilterAlgorithm {
    /// <summary>
    /// Block-based Bloom filter.
    /// </summary>
    public SplitBlockAlgorithm? BLOCK { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: BLOCK, id
        if(BLOCK != null) {
            proto.BeginInlineStruct(1);
            BLOCK.Write(proto);
        }

        proto.StructEnd();
    }

    internal static BloomFilterAlgorithm Read(ThriftCompactProtocolReader proto) {
        var r = new BloomFilterAlgorithm();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // BLOCK, id
                    r.BLOCK = SplitBlockAlgorithm.Read(proto);
                    break;
                default:
                    proto.SkipField(compactType);
                    break;
            }
        }
        proto.StructEnd();
        return r;
    }
}