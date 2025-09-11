using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Bloom filter header is stored at beginning of Bloom filter data of each column and followed by its bitset.
/// </summary>
public class BloomFilterHeader {
    /// <summary>
    /// The size of bitset in bytes.
    /// </summary>
    public int NumBytes { get; set; }

    /// <summary>
    /// The algorithm for setting bits.
    /// </summary>
    public BloomFilterAlgorithm Algorithm { get; set; } = new BloomFilterAlgorithm();

    /// <summary>
    /// The hash function used for Bloom filter.
    /// </summary>
    public BloomFilterHash Hash { get; set; } = new BloomFilterHash();

    /// <summary>
    /// The compression used in the Bloom filter.
    /// </summary>
    public BloomFilterCompression Compression { get; set; } = new BloomFilterCompression();


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: NumBytes, i32
        proto.WriteI32Field(1, NumBytes);
        // 2: Algorithm, id
        proto.BeginInlineStruct(2);
        Algorithm.Write(proto);
        // 3: Hash, id
        proto.BeginInlineStruct(3);
        Hash.Write(proto);
        // 4: Compression, id
        proto.BeginInlineStruct(4);
        Compression.Write(proto);

        proto.StructEnd();
    }

    internal static BloomFilterHeader Read(ThriftCompactProtocolReader proto) {
        var r = new BloomFilterHeader();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // NumBytes, i32
                    r.NumBytes = proto.ReadI32();
                    break;
                case 2: // Algorithm, id
                    r.Algorithm = BloomFilterAlgorithm.Read(proto);
                    break;
                case 3: // Hash, id
                    r.Hash = BloomFilterHash.Read(proto);
                    break;
                case 4: // Compression, id
                    r.Compression = BloomFilterCompression.Read(proto);
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