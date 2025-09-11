using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class BloomFilterCompression {
    public Uncompressed? UNCOMPRESSED { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: UNCOMPRESSED, id
        if(UNCOMPRESSED != null) {
            proto.BeginInlineStruct(1);
            UNCOMPRESSED.Write(proto);
        }

        proto.StructEnd();
    }

    internal static BloomFilterCompression Read(ThriftCompactProtocolReader proto) {
        var r = new BloomFilterCompression();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // UNCOMPRESSED, id
                    r.UNCOMPRESSED = Uncompressed.Read(proto);
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