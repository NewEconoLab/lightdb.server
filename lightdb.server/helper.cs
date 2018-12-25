using System;
using System.Collections.Generic;
using System.Text;

namespace LightDB.Server
{
    public static class Helper
    {
        [ThreadStatic]
        static System.Security.Cryptography.SHA256 _sha256;
        public static System.Security.Cryptography.SHA256 Sha256
        {
            get
            {
                if (_sha256 == null)
                    _sha256 = System.Security.Cryptography.SHA256.Create();
                return _sha256;
            }
        }

        public static ThinNeo.Hash256 CalcHash256(byte[] data)
        {
            var hash1 = Helper.Sha256.ComputeHash(data);
            var hash2 = Helper.Sha256.ComputeHash(hash1);
            return hash2;
        }

        public static bool BytesEquals(byte[] a1, byte[] a2)
        {
            if (ReferenceEquals(a1, a2))
                return true;
            if (a1 == null || a2 == null || a1.Length != a2.Length)
                return false;
            unsafe
            {
                fixed (byte* p1 = a1, p2 = a2)
                {
                    byte* x1 = p1, x2 = p2;
                    int l = a1.Length;
                    for (int i = 0; i < l / 8; i++, x1 += 8, x2 += 8)
                        if (*((long*)x1) != *((long*)x2)) return false;
                    if ((l & 4) != 0) { if (*((int*)x1) != *((int*)x2)) return false; x1 += 4; x2 += 4; }
                    if ((l & 2) != 0) { if (*((short*)x1) != *((short*)x2)) return false; x1 += 2; x2 += 2; }
                    if ((l & 1) != 0) if (*((byte*)x1) != *((byte*)x2)) return false;
                    return true;
                }
            }
        }
    }
}
