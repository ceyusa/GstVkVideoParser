/* VideoParser
 * Copyright (C) 2022 Igalia, S.L.
 *     Author: Víctor Jáquez <vjaquez@igalia.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.  You
 * may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include "videoparser.h"
#include "pipeline.h"

#include <vk_video/vulkan_video_codecs_common.h>

class GstVideoDecoderParser : public VulkanVideoDecodeParser
{
public:
    GstVideoDecoderParser()
        : m_refCount(1)
        , m_parser(nullptr) { }

    VkResult Initialize(VkParserInitDecodeParameters*) final;
    bool Deinitialize() final;
    bool ParseByteStream(const VkParserBitstreamPacket*, int32_t*) final;

    // not implemented
    bool DecodePicture(VkParserPictureData*) final { return false; }
    bool DecodeSliceInfo(VkParserSliceInfo*, const VkParserPictureData*, int32_t) final { return false; }
    bool GetDisplayMasteringInfo(VkParserDisplayMasteringInfo*) final { return false; }

    int32_t AddRef() final;
    int32_t Release() final;

private:
    ~GstVideoDecoderParser() {}

    int m_refCount;
    VkParserVideoDecodeClient *m_client;
    GstVideoParser *m_parser;
};

VkResult GstVideoDecoderParser::Initialize(VkParserInitDecodeParameters* params)
{
    if (!(params && params->interfaceVersion == VK_MAKE_VIDEO_STD_VERSION(0, 9, 1)))
        return VK_ERROR_INITIALIZATION_FAILED;

    if (!params->pClient)
        return VK_ERROR_INITIALIZATION_FAILED;

    if (!gst_init_check(NULL, NULL, NULL))
        return VK_ERROR_INITIALIZATION_FAILED;

    m_client = params->pClient;

    m_parser = gst_video_parser_new();

    return VK_SUCCESS;
}

bool GstVideoDecoderParser::Deinitialize()
{
    gst_clear_object (&m_parser);
    gst_deinit();
    return true;
}

bool GstVideoDecoderParser::ParseByteStream(const VkParserBitstreamPacket *bspacket, int32_t *parsed)
{
    if (parsed)
        *parsed = 0;

    auto buffer = gst_buffer_new_memdup (bspacket->pByteStream, bspacket->nDataLength);
    if (!buffer)
        return false;

    auto ret = gst_video_parser_push_buffer (m_parser, buffer);
    if (ret != GST_FLOW_OK)
        return false;

    if (bspacket->bEOS) {
        ret = gst_video_parser_eos (m_parser);
        if (ret != GST_FLOW_EOS)
            return false;
    }

    if (parsed)
        *parsed = bspacket->nDataLength;

    return true;
}

int32_t GstVideoDecoderParser::AddRef()
{
    g_atomic_int_inc(&m_refCount);
    return m_refCount;
}

int32_t GstVideoDecoderParser::Release()
{
    if (g_atomic_int_dec_and_test (&m_refCount)) {
        Deinitialize();
        delete this;
        return 0;
    }

    return m_refCount;
}

bool CreateVulkanVideoDecodeParser(VulkanVideoDecodeParser** parser, VkVideoCodecOperationFlagBitsKHR codec, ParserLogFuncType logfunc = nullptr, int loglevel = 0)
{
    GstVideoDecoderParser *internalParser = nullptr;

    if (!parser)
        return false;

    internalParser = new GstVideoDecoderParser();
    if (!internalParser)
        return false;

    *parser = internalParser;
    return true;
}

