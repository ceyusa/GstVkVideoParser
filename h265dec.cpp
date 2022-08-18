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

#include "h265dec.h"

#include <atomic>

#include "base.h"
#include "videoparser.h"
#include "videoutils.h"

#define GST_H265_DEC(obj)           ((GstH265Dec *) obj)
#define GST_H265_DEC_GET_CLASS(obj) (G_TYPE_INSTANCE_GET_CLASS((obj), G_TYPE_FROM_INSTANCE(obj), GstH265DecClass))
#define GST_H265_DEC_CLASS(klass)   ((GstH265DecClass *) klass)

GST_DEBUG_CATEGORY_EXTERN (gst_video_parser_debug);
#define GST_CAT_DEFAULT gst_video_parser_debug

static GstStaticPadTemplate sink_factory =
GST_STATIC_PAD_TEMPLATE ("sink", GST_PAD_SINK, GST_PAD_ALWAYS,
    GST_STATIC_CAPS ("video/x-h265"));

static GstStaticPadTemplate src_factory =
GST_STATIC_PAD_TEMPLATE ("src", GST_PAD_SRC, GST_PAD_ALWAYS,
    GST_STATIC_CAPS ("video/x-raw, format=(string)NV12"));

typedef struct _VkH265Picture VkH265Picture;
struct _VkH265Picture
{
  StdVideoH265HrdParameters hrd;
  StdVideoH265SequenceParameterSetVui vui;
  StdVideoH265SequenceParameterSet sps;
  StdVideoH265PictureParameterSet pps;
  StdVideoH265VideoParameterSet vps;
  StdVideoH265ScalingLists scaling_lists_sps, scaling_lists_pps;
};

typedef struct _GstH265Dec GstH265Dec;
struct _GstH265Dec
{
  GstH265Decoder parent;
  VkParserVideoDecodeClient *client;
  gboolean oob_pic_params;

  gint max_dpb_size;

  GstH265SPS last_sps;
  GstH265PPS last_pps;
  VkH265Picture vkp;
  GArray *refs;

  VkSharedBaseObj<VkParserVideoRefCountBase> spsclient, ppsclient;

  guint32 sps_update_count;
  guint32 pps_update_count;
};

struct VkPic
{
  VkPicIf *pic;
  VkParserPictureData data;
  GByteArray *bitstream;
  VkH265Picture vkp;
  uint8_t *slice_group_map;
  GArray *slice_offsets;
};

enum
{
  PROP_USER_DATA = 1,
  PROP_OOB_PIC_PARAMS,
};

G_DEFINE_FINAL_TYPE(GstH265Dec, gst_h265_dec, GST_TYPE_H265_DECODER)

static gpointer parent_class = NULL;

static VkPic
*vk_pic_new (VkPicIf * pic)
{
  VkPic *vkpic = g_new0 (struct VkPic, 1);
  uint32_t zero = 0;

  vkpic->pic = pic;
  vkpic->bitstream = g_byte_array_new ();
  vkpic->slice_offsets = g_array_new (FALSE, FALSE, sizeof (uint32_t));
  g_array_append_val (vkpic->slice_offsets, zero);
  return vkpic;
}

static void
vk_pic_free (gpointer data)
{
  VkPic *vkpic = static_cast<VkPic *>(data);

  vkpic->pic->Release ();
  g_byte_array_unref (vkpic->bitstream);
  g_array_unref (vkpic->slice_offsets);
  g_free (vkpic->slice_group_map);
  g_free (vkpic);
}

static bool
profile_is_svc (GstCaps * caps)
{
  const GstStructure *structure = gst_caps_get_structure (caps, 0);
  const gchar *profile = gst_structure_get_string (structure, "profile");

  return g_str_has_prefix (profile, "scalable");
}

static GstFlowReturn
gst_h265_dec_new_sequence (GstH265Decoder * decoder, const GstH265SPS * sps,
    gint max_dpb_size)
{
  GstH265Dec *self = GST_H265_DEC (decoder);
  GstVideoDecoder *dec = GST_VIDEO_DECODER (decoder);
  GstVideoCodecState *state;
  VkParserSequenceInfo seqInfo;
  guint dar_n = 0, dar_d = 0;

  seqInfo = (VkParserSequenceInfo) {
    .eCodec = VK_VIDEO_CODEC_OPERATION_DECODE_H265_BIT_EXT,
    .isSVC = profile_is_svc(decoder->input_state->caps),
    .frameRate = pack_framerate(GST_VIDEO_INFO_FPS_N(&decoder->input_state->info), GST_VIDEO_INFO_FPS_D(&decoder->input_state->info)),
    //.bProgSeq = sps->,
    .nCodedWidth = sps->width,
    .nCodedHeight = sps->height,
    .nMaxWidth = 0,
    .nMaxHeight = 0,
    .nChromaFormat = sps->chroma_format_idc, // Chroma Format (0=4:0:0, 1=4:2:0, 2=4:2:2, 3=4:4:4)
    .uBitDepthLumaMinus8 = sps->bit_depth_luma_minus8, // Luma bit depth (0=8bit)
    .uBitDepthChromaMinus8 = sps->bit_depth_chroma_minus8, // Chroma bit depth (0=8bit)
    .cbSequenceHeader = 0, // Number of bytes in SequenceHeaderData
    .nMinNumDecodeSurfaces = max_dpb_size + 1,
    // .SequenceHeaderData[1024];
    .pbSideData = nullptr,
    .cbSideData = 0,
  };

  // if (sps->frame_cropping_flag) {
  //   seqInfo.nDisplayWidth = sps->crop_rect_width;
  //   seqInfo.nDisplayHeight = sps->crop_rect_height;
  // } else {
  //   seqInfo.nDisplayWidth = sps->width;
  //   seqInfo.nDisplayHeight = sps->height;
  // }

  if (sps->vui_parameters_present_flag) {
    // seqInfo.uVideoFullRange = sps->vui_parameters.video_full_range_flag;        // 0=16-235, 1=0-255
    // if (sps->vui_parameters.nal_hrd_parameters_present_flag)
    //   seqInfo.lBitrate = sps->vui_parameters.nal_hrd_parameters.bit_rate_scale;
    // else if (sps->vui_parameters.vcl_hrd_parameters_present_flag)
    //   seqInfo.lBitrate = sps->vui_parameters.vcl_hrd_parameters.bit_rate_scale;
    // seqInfo.lVideoFormat = sps->vui_parameters.video_format;    // Video Format (VideoFormatXXX)
    // seqInfo.lColorPrimaries = sps->vui_parameters.colour_primaries;     // Colour Primaries (ColorPrimariesXXX)
    // seqInfo.lTransferCharacteristics =
    //     sps->vui_parameters.transfer_characteristics;
    // seqInfo.lMatrixCoefficients = sps->vui_parameters.matrix_coefficients;
  }

  if (gst_video_calculate_display_ratio (&dar_n, &dar_d,
          seqInfo.nDisplayWidth, seqInfo.nDisplayHeight,
          GST_VIDEO_INFO_PAR_N (&decoder->input_state->info),
          GST_VIDEO_INFO_PAR_D (&decoder->input_state->info), 1, 1)) {
    seqInfo.lDARWidth = dar_n;
    seqInfo.lDARHeight = dar_d;
  }

  if (self->client)
    self->max_dpb_size = self->client->BeginSequence (&seqInfo);

  state =
      gst_video_decoder_set_output_state (dec, GST_VIDEO_FORMAT_NV12,
      seqInfo.nDisplayWidth, seqInfo.nDisplayHeight, decoder->input_state);
  gst_video_codec_state_unref (state);

  gst_video_decoder_negotiate (dec);

  return GST_FLOW_OK;
}

static GstFlowReturn
gst_h265_dec_decode_slice (GstH265Decoder * decoder, GstH265Picture * picture,
    GstH265Slice * slice, GArray * ref_pic_list0, GArray * ref_pic_list1)
{
  VkPic *vkpic = static_cast<VkPic *>(gst_h265_picture_get_user_data(picture));
  static const uint8_t nal[] = { 0, 0, 1 };
  uint32_t offset;

  vkpic->data.nNumSlices++;
  // nvidia parser adds 000001 NAL unit identifier at every slice
  g_byte_array_append (vkpic->bitstream, nal, sizeof (nal));
  g_byte_array_append (vkpic->bitstream, slice->nalu.data + slice->nalu.offset,
      slice->nalu.size);
  // GST_MEMDUMP_OBJECT(decoder, "SLICE :", slice->nalu.data + slice->nalu.offset, slice->nalu.size);
  offset =
      g_array_index (vkpic->slice_offsets, uint32_t,
      vkpic->slice_offsets->len - 1) + slice->nalu.size + sizeof (nal);
  g_array_append_val (vkpic->slice_offsets, offset);

  return GST_FLOW_OK;
}

static GstFlowReturn
gst_h265_dec_new_picture (GstH265Decoder * decoder, GstVideoCodecFrame * frame,
    GstH265Picture * picture)
{
  GstH265Dec *self = GST_H265_DEC (decoder);
  VkPicIf *pic = nullptr;
  VkPic *vkpic;

  if (self->client) {
    if (!self->client->AllocPictureBuffer (&pic))
      return GST_FLOW_ERROR;
  }

  vkpic = vk_pic_new (pic);
  gst_h265_picture_set_user_data (picture, vkpic, vk_pic_free);

  frame->output_buffer = gst_buffer_new ();

  return GST_FLOW_OK;
}

static GstFlowReturn
gst_h265_dec_output_picture (GstH265Decoder * decoder,
    GstVideoCodecFrame * frame, GstH265Picture * picture)
{
  GstH265Dec *self = GST_H265_DEC (decoder);
  VkPic *vkpic = reinterpret_cast<VkPic *>(gst_h265_picture_get_user_data(picture));;

  if (self->client) {
    if (!self->client->DisplayPicture (vkpic->pic,
            picture->system_frame_number)) {
      gst_h265_picture_unref (picture);
      return GST_FLOW_ERROR;
    }
  }

  gst_h265_picture_unref (picture);
  return gst_video_decoder_finish_frame (GST_VIDEO_DECODER (decoder), frame);
}

static GstFlowReturn
gst_h265_dec_end_picture (GstH265Decoder * decoder, GstH265Picture * picture)
{
  GstH265Dec *self = GST_H265_DEC (decoder);
  VkPic *vkpic = reinterpret_cast<VkPic *>(gst_h265_picture_get_user_data(picture));
  gsize len;
  uint32_t *slice_offsets;
  GstFlowReturn ret = GST_FLOW_OK;

  vkpic->data.pBitstreamData = g_byte_array_steal (vkpic->bitstream, &len);
  vkpic->data.nBitstreamDataLen = static_cast<int32_t>(len);
  vkpic->data.pSliceDataOffsets = slice_offsets =
      static_cast <uint32_t *>(g_array_steal (vkpic->slice_offsets, NULL));

  if (self->client) {
    if (!self->client->DecodePicture (&vkpic->data))
      ret = GST_FLOW_ERROR;
  }

  g_free (vkpic->data.pBitstreamData);
  g_free (slice_offsets);

  return ret;
}

static void
fill_sps (GstH265SPS * sps, VkH265Picture * vkp)
{
}

static void
fill_pps (GstH265PPS * pps, VkH265Picture * vkp)
{
}

static GstFlowReturn
gst_h265_dec_start_picture (GstH265Decoder * decoder, GstH265Picture * picture,
    GstH265Slice * slice, GstH265Dpb * dpb)
{
  GstH265Dec *self = GST_H265_DEC (decoder);
  VkPic *vkpic =
      reinterpret_cast <VkPic *>(gst_h265_picture_get_user_data (picture));
  VkH265Picture *vkp = &self->vkp;
  GstH265PPS *pps = slice->header.pps;
  GstH265SPS *sps = pps->sps;

  if (!self->oob_pic_params
      || (self->sps_update_count == 0 && self->sps_update_count == 0)) {
    vkp = &vkpic->vkp;
    fill_sps (sps, vkp);
    fill_pps (pps, vkp);
  }

  vkpic->data = (VkParserPictureData) {
    .PicWidthInMbs = sps->width / 16, // Coded Frame Size
    .FrameHeightInMbs = sps->height / 16, // Coded Frame Height
    .pCurrPic = vkpic->pic,
    // .field_pic_flag = slice->header.field_pic_flag, // 0=frame picture, 1=field picture
    // .bottom_field_flag = slice->header.bottom_field_flag, // 0=top field, 1=bottom field (ignored if field_pic_flag=0)
    // .second_field = picture->second_field, // Second field of a complementary field pair
    // .progressive_frame = (picture->buffer_flags & GST_VIDEO_BUFFER_FLAG_INTERLACED) == 0, // Frame is progressive
    // .top_field_first = (picture->buffer_flags & GST_VIDEO_BUFFER_FLAG_TFF) != 0,
    // .repeat_first_field = 0, // For 3:2 pulldown (number of additional fields,
    //                          // 2=frame doubling, 4=frame tripling)
    // .ref_pic_flag = picture->ref_pic, // Frame is a reference frame
    // .intra_pic_flag =
    //     GST_H265_IS_I_SLICE (&slice->header)
    //     || GST_H265_IS_SI_SLICE (&slice->header), // Frame is entirely intra coded (no temporal dependencies)
    .chroma_format = sps->chroma_format_idc, // Chroma Format (should match sequence info)
    .picture_order_count = picture->pic_order_cnt, // picture order count (if known)

    .pbSideData = nullptr, // Encryption Info
    .nSideDataLen = 0, // Encryption Info length

    // Bitstream data
    //.nBitstreamDataLen, // Number of bytes in bitstream data buffer
    //.pBitstreamData, // Ptr to bitstream data for this picture (slice-layer)
    //.pSliceDataOffsets, // nNumSlices entries, contains offset of each slice
    // within the bitstream data buffer
  };

  VkParserHevcPictureData *h265 = &vkpic->data.CodecSpecific.hevc;
  *h265 = (VkParserHevcPictureData) {
    .pStdSps = &vkp->sps,
    .pSpsClientObject = self->spsclient,
    .pStdPps = &vkp->pps,
    .pPpsClientObject = self->ppsclient,
    .pic_parameter_set_id = static_cast<uint8_t>(pps->id),          // PPS ID
    // .seq_parameter_set_id = static_cast<uint8_t>(pps->sequence->id),          // SPS ID
    // .num_ref_idx_l0_active_minus1 = pps->num_ref_idx_l0_active_minus1,
    // .num_ref_idx_l1_active_minus1 = pps->num_ref_idx_l1_active_minus1,
    // .weighted_pred_flag = pps->weighted_pred_flag,
    // .weighted_bipred_idc = pps->weighted_bipred_idc,
    // .pic_init_qp_minus26 = pps->pic_init_qp_minus26,
    // .redundant_pic_cnt_present_flag = pps->redundant_pic_cnt_present_flag,
    // .deblocking_filter_control_present_flag =
    //     pps->deblocking_filter_control_present_flag,
    // .transform_8x8_mode_flag = pps->transform_8x8_mode_flag,
    // .MbaffFrameFlag =
    //     (sps->mb_adaptive_frame_field_flag && !slice->header.field_pic_flag),
    // .constrained_intra_pred_flag = pps->constrained_intra_pred_flag,
    // .entropy_coding_mode_flag = pps->entropy_coding_mode_flag,
    // .pic_order_present_flag = pps->pic_order_present_flag,
    // .chroma_qp_index_offset = pps->chroma_qp_index_offset,
    // .second_chroma_qp_index_offset =
    //     static_cast<int8_t>(pps->second_chroma_qp_index_offset),
    // .frame_num = picture->frame_num,
    // .CurrFieldOrderCnt =
    //     { picture->top_field_order_cnt, picture->bottom_field_order_cnt },
    // .fmo_aso_enable = pps->num_slice_groups_minus1 > 0,
    // .num_slice_groups_minus1 =
    //     static_cast<uint8_t>(pps->num_slice_groups_minus1),
    // .slice_group_map_type = pps->slice_group_map_type,
    // .pic_init_qs_minus26 = pps->pic_init_qp_minus26,
    // .slice_group_change_rate_minus1 = pps->slice_group_change_rate_minus1,
    // .pMb2SliceGroupMap =
    //     (vkpic->slice_group_map = get_slice_group_map (pps),
    //      vkpic->slice_group_map),
    // // DPB
    // VkParserH265DpbEntry dpb[16 + 1]; // List of reference frames within the DPB
  };

  /* reference frames */
  {
  }

  return GST_FLOW_OK;
}

static void
gst_h265_dec_unhandled_nalu (GstH265Decoder * decoder, const guint8 * data,
    guint32 size)
{
  GstH265Dec *self = GST_H265_DEC (decoder);

  if (self->client)
    self->client->UnhandledNALU (data, size);
}

static bool
sps_cmp (GstH265SPS * a, GstH265SPS * b)
{
#define CMP_FIELD(x) G_STMT_START { if (a->x != b->x) return false; } G_STMT_END
  CMP_FIELD (id);

  return true;
#undef CMP_FIELD
}

static bool
pps_cmp (GstH265PPS * a, GstH265PPS * b)
{
#define CMP_FIELD(x)  G_STMT_START { if (a->x != b->x) return false;  }  G_STMT_END
  CMP_FIELD (id);

  return true;
#undef CMP_FIELD
}

static void
gst_h265_dec_update_picture_parameters (GstH265Decoder * decoder,
    GstH265NalUnitType type, const gpointer nalu)
{
  GstH265Dec *self = GST_H265_DEC (decoder);
  VkPictureParameters params;

  switch (type) {
    case GST_H265_NAL_SPS:{
      GstH265SPS *sps = static_cast < GstH265SPS * >(nalu);
      if (sps_cmp (&self->last_sps, sps))
        return;
      self->last_sps = *sps;
      fill_sps (sps, &self->vkp);
      params = (VkPictureParameters) {
        .updateType = VK_PICTURE_PARAMETERS_UPDATE_H265_SPS,
        .pH265Sps = &self->vkp.sps,
        .updateSequenceCount = self->sps_update_count++,
      };
      if (self->client) {
        if (!self->client->UpdatePictureParameters (&params, self->spsclient,
                params.updateSequenceCount))
          GST_ERROR_OBJECT (self, "Failed to update sequence parameters");
      }
      break;
    }
    case GST_H265_NAL_PPS:{
      GstH265PPS *pps = static_cast < GstH265PPS * >(nalu);
      if (pps_cmp (&self->last_pps, pps))
        return;
      self->last_pps = *pps;
      fill_pps (pps, &self->vkp);
      params = (VkPictureParameters) {
        .updateType = VK_PICTURE_PARAMETERS_UPDATE_H265_PPS,
        .pH265Pps = &self->vkp.pps,
        .updateSequenceCount = self->pps_update_count++,
      };
      if (self->client) {
        if (!self->client->UpdatePictureParameters (&params, self->ppsclient,
                params.updateSequenceCount))
          GST_ERROR_OBJECT (self, "Failed to update picture parameters");
      }
      break;
    }
    case GST_H265_NAL_VPS:{
      GstH265VPS *vps = static_cast < GstH265VPS * >(nalu);
      // if (pps_cmp (&self->last_pps, pps))
      //   return;
      // self->last_pps = *pps;
      // fill_pps (pps, &self->vkp);
      params = (VkPictureParameters) {
        .updateType = VK_PICTURE_PARAMETERS_UPDATE_H265_VPS,
        .pH265Vps = &self->vkp.vps,
        .updateSequenceCount = self->pps_update_count++,
      };
      if (self->client) {
        if (!self->client->UpdatePictureParameters (&params, self->ppsclient,
                params.updateSequenceCount))
          GST_ERROR_OBJECT (self, "Failed to update picture parameters");
      }
      break;
    }
    default:
      break;
  }
}

static void
gst_h265_dec_dispose (GObject * object)
{
  GstH265Dec *self = GST_H265_DEC (object);

  if (self->spsclient)
    self->spsclient->Release ();
  if (self->ppsclient)
    self->ppsclient->Release ();

  g_clear_pointer (&self->refs, g_array_unref);

  G_OBJECT_CLASS (parent_class)->dispose (object);
}

static void
gst_h265_dec_set_property (GObject * object, guint property_id,
    const GValue * value, GParamSpec * pspec)
{
  GstH265Dec *self = GST_H265_DEC (object);

  switch (property_id) {
    case PROP_USER_DATA:
      self->client =
          reinterpret_cast <VkParserVideoDecodeClient *>(g_value_get_pointer (value));
      break;
    case PROP_OOB_PIC_PARAMS:
      self->oob_pic_params = g_value_get_boolean (value);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
      break;
  }
}

static void
gst_h265_dec_class_init (GstH265DecClass * klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
  GstElementClass *element_class = GST_ELEMENT_CLASS (klass);
  GstH265DecoderClass *h265decoder_class = GST_H265_DECODER_CLASS (klass);

  parent_class = g_type_class_peek_parent (klass);

  gst_element_class_add_pad_template (element_class,
      gst_static_pad_template_get (&sink_factory));
  gst_element_class_add_pad_template (element_class,
      gst_static_pad_template_get (&src_factory));

  gobject_class->dispose = gst_h265_dec_dispose;
  gobject_class->set_property = gst_h265_dec_set_property;

  h265decoder_class->new_sequence = gst_h265_dec_new_sequence;
  h265decoder_class->decode_slice = gst_h265_dec_decode_slice;
  h265decoder_class->new_picture = gst_h265_dec_new_picture;
  h265decoder_class->output_picture = gst_h265_dec_output_picture;
  h265decoder_class->start_picture = gst_h265_dec_start_picture;
  h265decoder_class->end_picture = gst_h265_dec_end_picture;
  h265decoder_class->unhandled_nalu = gst_h265_dec_unhandled_nalu;
  h265decoder_class->update_picture_parameters =
      gst_h265_dec_update_picture_parameters;

  g_object_class_install_property (gobject_class, PROP_USER_DATA,
      g_param_spec_pointer ("user-data", "user-data", "user-data",
          GParamFlags (G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY)));

  g_object_class_install_property (gobject_class, PROP_OOB_PIC_PARAMS,
      g_param_spec_boolean ("oob-pic-params", "oob-pic-params",
          "oop-pic-params", FALSE,
          GParamFlags (G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY)));
}

static void
gst_h265_dec_init (GstH265Dec * self)
{
  gst_h265_decoder_set_process_ref_pic_lists (GST_H265_DECODER (self), FALSE);

  self->refs = g_array_sized_new (FALSE, TRUE, sizeof (GstH265Decoder *), 16);
  g_array_set_clear_func (self->refs, (GDestroyNotify) gst_clear_h265_picture);
}