using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// The hash function used in Bloom filter. This function takes the hash of a column value using plain encoding.
/// </summary>
public class BloomFilterHash {
    /// <summary>
    /// XxHash Strategy.
    /// </summary>
    public XxHash? XXHASH { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: XXHASH, id
        if(XXHASH != null) {
            proto.BeginInlineStruct(1);
            XXHASH.Write(proto);
        }

        proto.StructEnd();
    }

    internal static BloomFilterHash Read(ThriftCompactProtocolReader proto) {
        var r = new BloomFilterHash();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // XXHASH, id
                    r.XXHASH = XxHash.Read(proto);
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