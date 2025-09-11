using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Time logical type annotation  Allowed for physical types: INT32 (millis), INT64 (micros, nanos).
/// </summary>
public class TimeType {
    public bool IsAdjustedToUTC { get; set; }

    public TimeUnit Unit { get; set; } = new TimeUnit();


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: IsAdjustedToUTC, bool
        proto.WriteBoolField(1, IsAdjustedToUTC);
        // 2: Unit, id
        proto.BeginInlineStruct(2);
        Unit.Write(proto);

        proto.StructEnd();
    }

    internal static TimeType Read(ThriftCompactProtocolReader proto) {
        var r = new TimeType();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // IsAdjustedToUTC, bool
                    r.IsAdjustedToUTC = compactType == CompactType.BooleanTrue;
                    break;
                case 2: // Unit, id
                    r.Unit = TimeUnit.Read(proto);
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