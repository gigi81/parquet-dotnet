using System.IO;
using System.Threading.Tasks;

namespace Parquet.Extensions;

internal static class StreamExtensions {
    public static byte[] ReadBytesExactly(this Stream s, int count) {
        byte[] tmp = new byte[count];
        int read = 0;
        while(read < count) {
            int r = s.Read(tmp, read, count - read);
            if(r == 0)
                break;
            else
                read += r;
        }
        if(read < count)
            throw new IOException($"only {read} out of {count} bytes are available");
        return tmp;
    }

    public static async Task<byte[]> ReadBytesExactlyAsync(this Stream s, int count) {
        byte[] tmp = new byte[count];
#if NET7_0_OR_GREATER
            await s.ReadExactlyAsync(tmp, 0, count);
#else
        int read = 0;
        while(read < count) {
            int r = await s.ReadAsync(tmp, read, count - read);
            if(r == 0)
                break;
            else
                read += r;
        }
        if(read < count)
            throw new IOException($"only {read} out of {count} bytes are available");
#endif

        return tmp;
    }
}