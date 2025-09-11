using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// The dictionary page must be placed at the first position of the column chunk if it is partly or completely dictionary encoded. At most one dictionary page can be placed in a column chunk.
/// </summary>
public class DictionaryPageHeader {
    /// <summary>
    /// Number of values in the dictionary.
    /// </summary>
    public int NumValues { get; set; }

    /// <summary>
    /// Encoding using this dictionary page.
    /// </summary>
    public Encoding Encoding { get; set; } = new Encoding();

    /// <summary>
    /// If true, the entries in the dictionary are sorted in ascending order.
    /// </summary>
    public bool? IsSorted { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: NumValues, i32
        proto.WriteI32Field(1, NumValues);
        // 2: Encoding, id
        proto.WriteI32Field(2, (int)Encoding);
        // 3: IsSorted, bool
        if(IsSorted != null) {
            proto.WriteBoolField(3, IsSorted.Value);
        }

        proto.StructEnd();
    }

    internal static DictionaryPageHeader Read(ThriftCompactProtocolReader proto) {
        var r = new DictionaryPageHeader();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // NumValues, i32
                    r.NumValues = proto.ReadI32();
                    break;
                case 2: // Encoding, id
                    r.Encoding = (Encoding)proto.ReadI32();
                    break;
                case 3: // IsSorted, bool
                    r.IsSorted = compactType == CompactType.BooleanTrue;
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