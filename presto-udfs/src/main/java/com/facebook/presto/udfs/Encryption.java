/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.udfs;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.SecretKeySpec;

import static com.facebook.presto.common.type.encoding.StringUtils.UTF_8;

public class Encryption
{
    private static String cipherTransformation = "AES/ECB/PKCS5Padding";
    private static String algorithm = "AES";
    private static String version = "V2";
    private static final Cipher cipher;

    static {
        try {
            cipher = Cipher.getInstance(cipherTransformation);
            String cipherVersionValue = "1234567890123456";
            SecretKeySpec keySpec = new SecretKeySpec(cipherVersionValue.getBytes(),
                    algorithm);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Encryption()
    {
    }

    @Description("encrypts the string.")
    @ScalarFunction("default_encrypt")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice encrypt(@SqlType(StandardTypes.VARCHAR) Slice str)
            throws BadPaddingException, IllegalBlockSizeException
    {
        if (str == null) {
            return null;
        }
        return Slices.copiedBuffer((Base64.encodeBase64String(cipher
                .doFinal(str.toStringUtf8().trim()
                        .replaceAll("[\n\r]", "").getBytes())).concat(version)), UTF_8);
    }
}
