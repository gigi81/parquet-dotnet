using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Integer logical type annotation  bitWidth must be 8, 16, 32, or 64.  Allowed for physical types: INT32, INT64.
/// </summary>
public class IntType {
    public sbyte BitWidth { get; set; }

    public bool IsSigned { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: BitWidth, i8
        proto.WriteByteField(1, BitWidth);
        // 2: IsSigned, bool
        proto.WriteBoolField(2, IsSigned);

        proto.StructEnd();
    }

    internal static IntType Read(ThriftCompactProtocolReader proto) {
        var r = new IntType();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // BitWidth, i8
                    r.BitWidth = proto.ReadByte();
                    break;
                case 2: // IsSigned, bool
                    r.IsSigned = compactType == CompactType.BooleanTrue;
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