/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hadoop.compression.lzo;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LzoCodec extends io.airlift.compress.lzo.LzoCodec {
    private static final Log LOG = LogFactory.getLog(LzoCodec.class);

    static final String gplLzoCodec = LzoCodec.class.getName();
    static final String airliftLzoCodec = io.airlift.compress.lzo.LzoCodec.class.getName();
    static boolean warned = false;

    static {
        LOG.info("Bridging " + gplLzoCodec + " to " + airliftLzoCodec + ".");
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out,
                                                      Compressor compressor) throws IOException {
        if (!warned) {
            LOG.warn(gplLzoCodec + " is deprecated. You should use " + airliftLzoCodec
                    + " instead to generate LZO compressed data.");
            warned = true;
        }
        return super.createOutputStream(out, compressor);
    }
}
