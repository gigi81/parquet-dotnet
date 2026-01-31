using System.Runtime.InteropServices;
using Xunit;

namespace Parquet.Test.Xunit {
    public class SkipOnWindowsX86TheoryAttribute : TheoryAttribute {
        public SkipOnWindowsX86TheoryAttribute() {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) &&
                RuntimeInformation.ProcessArchitecture == Architecture.X86) {
                Skip = "Excluded on Windows x86.";
            }
        }
    }
}

