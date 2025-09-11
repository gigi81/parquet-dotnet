using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// Wrapper struct to store key values.
/// </summary>
public class KeyValue {
    public string Key { get; set; } = string.Empty;

    public string? Value { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: Key, string
        proto.WriteStringField(1, Key ?? string.Empty);
        // 2: Value, string
        if(Value != null) {
            proto.WriteStringField(2, Value);
        }

        proto.StructEnd();
    }

    internal static KeyValue Read(ThriftCompactProtocolReader proto) {
        var r = new KeyValue();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // Key, string
                    r.Key = proto.ReadString();
                    break;
                case 2: // Value, string
                    r.Value = proto.ReadString();
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