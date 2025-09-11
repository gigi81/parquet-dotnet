using Parquet.Meta.Proto;

namespace Parquet.Meta;

public class TimeUnit {
    public MilliSeconds? MILLIS { get; set; }

    public MicroSeconds? MICROS { get; set; }

    public NanoSeconds? NANOS { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: MILLIS, id
        if(MILLIS != null) {
            proto.BeginInlineStruct(1);
            MILLIS.Write(proto);
        }
        // 2: MICROS, id
        if(MICROS != null) {
            proto.BeginInlineStruct(2);
            MICROS.Write(proto);
        }
        // 3: NANOS, id
        if(NANOS != null) {
            proto.BeginInlineStruct(3);
            NANOS.Write(proto);
        }

        proto.StructEnd();
    }

    internal static TimeUnit Read(ThriftCompactProtocolReader proto) {
        var r = new TimeUnit();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // MILLIS, id
                    r.MILLIS = MilliSeconds.Read(proto);
                    break;
                case 2: // MICROS, id
                    r.MICROS = MicroSeconds.Read(proto);
                    break;
                case 3: // NANOS, id
                    r.NANOS = NanoSeconds.Read(proto);
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