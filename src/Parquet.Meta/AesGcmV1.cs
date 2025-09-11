using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class AesGcmV1 {
    /// <summary>
    /// AAD prefix.
    /// </summary>
    public byte[]? AadPrefix { get; set; }

    /// <summary>
    /// Unique file identifier part of AAD suffix.
    /// </summary>
    public byte[]? AadFileUnique { get; set; }

    /// <summary>
    /// In files encrypted with AAD prefix without storing it, readers must supply the prefix.
    /// </summary>
    public bool? SupplyAadPrefix { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: AadPrefix, binary
        if(AadPrefix != null) {
            proto.WriteBinaryField(1, AadPrefix);
        }
        // 2: AadFileUnique, binary
        if(AadFileUnique != null) {
            proto.WriteBinaryField(2, AadFileUnique);
        }
        // 3: SupplyAadPrefix, bool
        if(SupplyAadPrefix != null) {
            proto.WriteBoolField(3, SupplyAadPrefix.Value);
        }

        proto.StructEnd();
    }

    internal static AesGcmV1 Read(ThriftCompactProtocolReader proto) {
        var r = new AesGcmV1();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // AadPrefix, binary
                    r.AadPrefix = proto.ReadBinary();
                    break;
                case 2: // AadFileUnique, binary
                    r.AadFileUnique = proto.ReadBinary();
                    break;
                case 3: // SupplyAadPrefix, bool
                    r.SupplyAadPrefix = compactType == CompactType.BooleanTrue;
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