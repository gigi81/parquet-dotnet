using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Decimal logical type annotation  Scale must be zero or a positive integer less than or equal to the precision. Precision must be a non-zero positive integer.  To maintain forward-compatibility in v1, implementations using this logical type must also set scale and precision on the annotated SchemaElement.  Allowed for physical types: INT32, INT64, FIXED_LEN_BYTE_ARRAY, and BYTE_ARRAY.
/// </summary>
public class DecimalType {
    public int Scale { get; set; }

    public int Precision { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: Scale, i32
        proto.WriteI32Field(1, Scale);
        // 2: Precision, i32
        proto.WriteI32Field(2, Precision);

        proto.StructEnd();
    }

    internal static DecimalType Read(ThriftCompactProtocolReader proto) {
        var r = new DecimalType();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // Scale, i32
                    r.Scale = proto.ReadI32();
                    break;
                case 2: // Precision, i32
                    r.Precision = proto.ReadI32();
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