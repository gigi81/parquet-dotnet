using Parquet.Meta.Proto;

namespace Parquet.Meta;

/// <summary>
/// LogicalType annotations to replace ConvertedType.  To maintain compatibility, implementations using LogicalType for a SchemaElement must also set the corresponding ConvertedType (if any) from the following table.
/// </summary>
public class LogicalType {
    public StringType? STRING { get; set; }

    public MapType? MAP { get; set; }

    public ListType? LIST { get; set; }

    public EnumType? ENUM { get; set; }

    public DecimalType? DECIMAL { get; set; }

    public DateType? DATE { get; set; }

    public TimeType? TIME { get; set; }

    public TimestampType? TIMESTAMP { get; set; }

    public IntType? INTEGER { get; set; }

    public NullType? UNKNOWN { get; set; }

    public JsonType? JSON { get; set; }

    public BsonType? BSON { get; set; }

    public UUIDType? UUID { get; set; }

    public Float16Type? FLOAT16 { get; set; }

    public VariantType? VARIANT { get; set; }


    internal void Write(ThriftCompactProtocolWriter proto) {
        proto.StructBegin();

        // 1: STRING, id
        if(STRING != null) {
            proto.BeginInlineStruct(1);
            STRING.Write(proto);
        }
        // 2: MAP, id
        if(MAP != null) {
            proto.BeginInlineStruct(2);
            MAP.Write(proto);
        }
        // 3: LIST, id
        if(LIST != null) {
            proto.BeginInlineStruct(3);
            LIST.Write(proto);
        }
        // 4: ENUM, id
        if(ENUM != null) {
            proto.BeginInlineStruct(4);
            ENUM.Write(proto);
        }
        // 5: DECIMAL, id
        if(DECIMAL != null) {
            proto.BeginInlineStruct(5);
            DECIMAL.Write(proto);
        }
        // 6: DATE, id
        if(DATE != null) {
            proto.BeginInlineStruct(6);
            DATE.Write(proto);
        }
        // 7: TIME, id
        if(TIME != null) {
            proto.BeginInlineStruct(7);
            TIME.Write(proto);
        }
        // 8: TIMESTAMP, id
        if(TIMESTAMP != null) {
            proto.BeginInlineStruct(8);
            TIMESTAMP.Write(proto);
        }
        // 10: INTEGER, id
        if(INTEGER != null) {
            proto.BeginInlineStruct(10);
            INTEGER.Write(proto);
        }
        // 11: UNKNOWN, id
        if(UNKNOWN != null) {
            proto.BeginInlineStruct(11);
            UNKNOWN.Write(proto);
        }
        // 12: JSON, id
        if(JSON != null) {
            proto.BeginInlineStruct(12);
            JSON.Write(proto);
        }
        // 13: BSON, id
        if(BSON != null) {
            proto.BeginInlineStruct(13);
            BSON.Write(proto);
        }
        // 14: UUID, id
        if(UUID != null) {
            proto.BeginInlineStruct(14);
            UUID.Write(proto);
        }
        // 15: FLOAT16, id
        if(FLOAT16 != null) {
            proto.BeginInlineStruct(15);
            FLOAT16.Write(proto);
        }
        // 16: VARIANT, id
        if(VARIANT != null) {
            proto.BeginInlineStruct(16);
            VARIANT.Write(proto);
        }

        proto.StructEnd();
    }

    internal static LogicalType Read(ThriftCompactProtocolReader proto) {
        var r = new LogicalType();
        proto.StructBegin();
        while(proto.ReadNextField(out short fieldId, out CompactType compactType)) {
            switch(fieldId) {
                case 1: // STRING, id
                    r.STRING = StringType.Read(proto);
                    break;
                case 2: // MAP, id
                    r.MAP = MapType.Read(proto);
                    break;
                case 3: // LIST, id
                    r.LIST = ListType.Read(proto);
                    break;
                case 4: // ENUM, id
                    r.ENUM = EnumType.Read(proto);
                    break;
                case 5: // DECIMAL, id
                    r.DECIMAL = DecimalType.Read(proto);
                    break;
                case 6: // DATE, id
                    r.DATE = DateType.Read(proto);
                    break;
                case 7: // TIME, id
                    r.TIME = TimeType.Read(proto);
                    break;
                case 8: // TIMESTAMP, id
                    r.TIMESTAMP = TimestampType.Read(proto);
                    break;
                case 10: // INTEGER, id
                    r.INTEGER = IntType.Read(proto);
                    break;
                case 11: // UNKNOWN, id
                    r.UNKNOWN = NullType.Read(proto);
                    break;
                case 12: // JSON, id
                    r.JSON = JsonType.Read(proto);
                    break;
                case 13: // BSON, id
                    r.BSON = BsonType.Read(proto);
                    break;
                case 14: // UUID, id
                    r.UUID = UUIDType.Read(proto);
                    break;
                case 15: // FLOAT16, id
                    r.FLOAT16 = Float16Type.Read(proto);
                    break;
                case 16: // VARIANT, id
                    r.VARIANT = VariantType.Read(proto);
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