/* DemuxerES
 * Copyright (C) 2022 Igalia, S.L.
 *     Author: Stephane Cerveau <scerveau@igalia.com>
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

#include "gstdemuxeres.h"

#include <gst/gst.h>
#include <gst/app/gstappsink.h>

static guint packet_counter = 0;

GST_DEBUG_CATEGORY_STATIC (demuxer_es_debug);
#define GST_CAT_DEFAULT demuxer_es_debug


typedef enum
{
  GST_AUTOPLUG_SELECT_TRY,
  GST_AUTOPLUG_SELECT_EXPOSE,
  GST_AUTOPLUG_SELECT_SKIP
} GstAutoplugSelectResult;

typedef enum _GstDemuxerESState
{
  DEMUXER_ES_STATE_IDLE = 0,
  DEMUXER_ES_STATE_ERROR,
  DEMUXER_ES_STATE_READY,
  DEMUXER_ES_STATE_EOS,
} GstDemuxerESState;

struct _GstDemuxerESPrivate
{
  GstElement *pipeline;
  GstElement *parsebin;
  GstElement *funnel;
  GstElement *appsink;

  gint current_stream_id;
  gint current_stream_type;

  GList *streams;

  GstDemuxerESState state;
  GCond ready_cond;
  GMutex ready_mutex;

  GThread *bus_thread;
  gboolean bus_exit;
  GstSample *pending_sample;
};


static gchar *
get_gst_valid_uri (const gchar * filename)
{
  if (gst_uri_is_valid (filename)) {
    return g_strdup (filename);
  }

  return gst_filename_to_uri (filename, NULL);
}

struct _GstDemuxerESPacketPrivate
{
  GstSample *sample;
  GstMapInfo map;
};


static inline void
set_demuxer_state (GstDemuxerES * demuxer, GstDemuxerESState state)
{
  GstDemuxerESPrivate *priv = demuxer->priv;
  g_mutex_lock (&priv->ready_mutex);
  GST_LOG ("Set state %d", state);
  priv->state = state;
  g_cond_signal (&priv->ready_cond);
  g_mutex_unlock (&priv->ready_mutex);
}

static GstDemuxerEStream *
find_stream (GstDemuxerES * demuxer, const gchar * stream_id)
{
  GstDemuxerESPrivate *priv = demuxer->priv;
  GList *l;

  for (l = priv->streams; l != NULL; l = g_list_next (l)) {
    GstDemuxerEStream *stream = l->data;
    if (!g_strcmp0 (stream->stream_id, stream_id)) {
      return stream;
    }
  }
  return NULL;
}

static gboolean
appsink_handle_event (GstDemuxerES * demuxer, GstEvent * event)
{
  GstDemuxerESPrivate *priv = demuxer->priv;
  gboolean ret = FALSE;
  GST_LOG ("%" GST_PTR_FORMAT, event);
  switch (GST_EVENT_TYPE (event)) {
      // Each time the funnel receives this event it will tell the next buffer type and id
      // by changing the global current value.
    case GST_EVENT_STREAM_START:{
      const gchar *stream_id;
      GstDemuxerEStream *stream;
      gst_event_parse_stream_start (event, &stream_id);
      stream = find_stream (demuxer, stream_id);
      if (stream) {
        priv->current_stream_id = stream->id;
        priv->current_stream_type = stream->type;
      } else {
        GST_WARNING
            ("Received GST_EVENT_STREAM_START for an unknown stream id %s",
            stream_id);
      }
      ret = TRUE;
      break;
    }
    case GST_EVENT_STREAM_GROUP_DONE:{
      set_demuxer_state (demuxer, DEMUXER_ES_STATE_EOS);
      ret = TRUE;
      break;
    }
    default:
      break;
  }
  gst_event_unref (event);
  return ret;
}

static GstDemuxerESPacket *
appsink_read_packet (GstDemuxerES * demuxer)
{
  GstSample *sample = NULL;
  GstBuffer *buffer;
  GstDemuxerESPacket *packet = NULL;
  GstDemuxerESPrivate *priv = demuxer->priv;
  GstMiniObject *object;

  if (!priv->pending_sample) {
    while ((object =
            gst_app_sink_try_pull_object (GST_APP_SINK (priv->appsink), -1))) {
      if (GST_IS_EVENT (object)) {
        appsink_handle_event (demuxer, GST_EVENT (object));
      } else if (GST_IS_SAMPLE (object)) {
        sample = GST_SAMPLE (object);
        break;
      }
    }
  } else {
    sample = priv->pending_sample;
    priv->pending_sample = NULL;
  }

  if (sample) {
    buffer = gst_sample_get_buffer (sample);
    if (buffer) {
      packet = g_new (GstDemuxerESPacket, 1);
      *packet = (GstDemuxerESPacket) {
        .priv = NULL,.stream_type = priv->current_stream_type,.stream_id =
            priv->current_stream_id,.packet_number = packet_counter,.pts =
            GST_BUFFER_PTS (buffer),.dts = GST_BUFFER_DTS (buffer),.duration =
            GST_BUFFER_DURATION (buffer)
      };
      packet->priv = g_new (GstDemuxerESPacketPrivate, 1),
          packet->priv->sample = sample;
      packet_counter++;
      gst_buffer_map (buffer, &packet->priv->map, GST_MAP_READ);
      packet->data = packet->priv->map.data;
      packet->data_size = packet->priv->map.size;
      GST_LOG ("A new packet of size %ld is available", packet->data_size);
    }
  } else {
    GST_ERROR ("no sample available");
  }
  //Look for stream_start or eos event
  while ((object =
          gst_app_sink_try_pull_object (GST_APP_SINK (priv->appsink), -1))) {
    if (GST_IS_EVENT (object)) {
      if (appsink_handle_event (demuxer, GST_EVENT (object))) {
        break;
      }
    } else if (GST_IS_SAMPLE (object)) {
      priv->pending_sample = GST_SAMPLE (object);
      break;
    }
  }

  return packet;
}

static GstDemuxerEStreamType
gst_parse_stream_get_type_from_pad (GstPad * pad)
{
  GstCaps *caps;
  const GstStructure *s;
  const gchar *name;
  GstDemuxerEStreamType type = DEMUXER_ES_STREAM_TYPE_UNKNOWN;

  caps = gst_pad_query_caps (pad, NULL);

  if (caps) {
    s = gst_caps_get_structure (caps, 0);
    name = gst_structure_get_name (s);
    if (g_str_has_prefix (name, "video")) {
      type = DEMUXER_ES_STREAM_TYPE_VIDEO;
    } else if (g_str_has_prefix (name, "audio")) {
      type = DEMUXER_ES_STREAM_TYPE_AUDIO;
    } else if (g_str_has_prefix (name, "text")) {
      type = DEMUXER_ES_STREAM_TYPE_TEXT;
    }
    gst_caps_unref (caps);
  }
  return type;
}

static GstDemuxerESVideoCodec
gst_parse_stream_get_vcodec_from_caps (GstCaps * caps)
{
  GstDemuxerESVideoCodec ret = DEMUXER_ES_VIDEO_CODEC_UNKNOWN;
  GstStructure *s;
  const gchar *name;

  if (!caps)
    return ret;

  s = gst_caps_get_structure (caps, 0);
  if (!s)
    goto beach;

  name = gst_structure_get_name (s);
  if (!name)
    goto beach;

  if (!strcmp (name, "video/x-h264")) {
    ret = DEMUXER_ES_VIDEO_CODEC_H264;
  } else if (!strcmp (name, "video/x-h265")) {
    ret = DEMUXER_ES_VIDEO_CODEC_H265;
  } else if (!strcmp (name, "video/x-av1")) {
    ret = DEMUXER_ES_VIDEO_CODEC_AV1;
  } else if (!strcmp (name, "video/x-vp9")) {
    ret = DEMUXER_ES_VIDEO_CODEC_VP9;
  }

beach:
  return ret;
}

static GstDemuxerESAudioCodec
gst_parse_stream_get_acodec_from_caps (GstCaps * caps)
{
  GstDemuxerESAudioCodec ret = DEMUXER_ES_AUDIO_CODEC_UNKNOWN;
  GstStructure *s;
  const gchar *name;

  if (!caps)
    return ret;

  s = gst_caps_get_structure (caps, 0);
  if (!s)
    goto beach;

  name = gst_structure_get_name (s);
  if (!name)
    goto beach;

  if (!strcmp (name, "audio/x-aac")) {
    ret = DEMUXER_ES_AUDIO_CODEC_AAC;
  }

beach:
  return ret;
}

void
gst_parse_stream_teardown (GstDemuxerEStream * stream)
{
  switch (stream->type) {
    case DEMUXER_ES_STREAM_TYPE_VIDEO:
      g_free (stream->data.video.profile);
      g_free (stream->data.video.level);
      break;
    default:
      break;
  }
  g_free (stream->stream_id);
  g_free (stream);
}

GstDemuxerEStream *
gst_parse_stream_create (GstDemuxerES * demuxer, GstPad * pad)
{
  GstDemuxerEStream *stream;
  GstDemuxerESPrivate *priv = demuxer->priv;
  GstCaps *caps = gst_pad_query_caps (pad, NULL);

  if (!caps) {
    GST_ERROR
        ("Unable to get the caps from pad, unable to create a new stream");
    return NULL;
  }
  GST_INFO ("the stream caps is %" GST_PTR_FORMAT, caps);

  stream = g_new0 (GstDemuxerEStream, 1);
  stream->demuxer = demuxer;
  stream->type = gst_parse_stream_get_type_from_pad (pad);
  stream->stream_id = gst_pad_get_stream_id (GST_PAD_CAST (pad));
  GstStructure *s = gst_caps_get_structure (caps, 0);
  switch (stream->type) {
    case DEMUXER_ES_STREAM_TYPE_VIDEO:
    {
      gst_video_info_from_caps (&stream->data.video.info, caps);
      gst_structure_get_int (s, "bitrate", &stream->data.video.bitrate);
      stream->data.video.profile =
          g_strdup (gst_structure_get_string (s, "profile"));
      stream->data.video.level =
          g_strdup (gst_structure_get_string (s, "level"));
      stream->data.video.vcodec = gst_parse_stream_get_vcodec_from_caps (caps);
      break;
    }
    case DEMUXER_ES_STREAM_TYPE_AUDIO:
    {
      gst_audio_info_from_caps (&stream->data.audio.info, caps);
      gst_structure_get_int (s, "bitrate", &stream->data.audio.bitrate);
      stream->data.audio.acodec = gst_parse_stream_get_acodec_from_caps (caps);
      break;
    }
    default:
      break;
  }
  gst_caps_unref (caps);

  stream->id = g_list_length (priv->streams);
  return stream;
}

static inline gboolean
wait_for_demuxer_ready (GstDemuxerES * demuxer)
{
  GstDemuxerESPrivate *priv = demuxer->priv;

  g_mutex_lock (&priv->ready_mutex);
  while (priv->state == DEMUXER_ES_STATE_IDLE) {
    g_cond_wait (&priv->ready_cond, &priv->ready_mutex);
  }
  g_mutex_unlock (&priv->ready_mutex);

  return (priv->state >= DEMUXER_ES_STATE_READY);
}

static void
parsebin_pad_added_cb (GstElement * parsebin, GstPad * pad,
    GstDemuxerES * demuxer)
{
  GstDemuxerEStream *stream = NULL;
  GstPad *funnel_pad;
  GstDemuxerESPrivate *priv = demuxer->priv;

  if (!GST_PAD_IS_SRC (pad))
    return;

  GST_DEBUG ("pad %s:%s", GST_DEBUG_PAD_NAME (pad));

  stream = gst_parse_stream_create (demuxer, pad);
  if (!stream)
    return;

  demuxer->priv->streams = g_list_append (demuxer->priv->streams, stream);

  funnel_pad = gst_element_request_pad_simple (priv->funnel, "sink_%u");

  if (gst_pad_link (pad, funnel_pad) != GST_PAD_LINK_OK)
    GST_ERROR ("Unable to plug the pad %p to the capsfilter pad", pad);
  gst_object_unref (funnel_pad);

  GST_DEBUG_BIN_TO_DOT_FILE_WITH_TS (GST_BIN (priv->pipeline),
      GST_DEBUG_GRAPH_SHOW_ALL, "gst-demuxerer.stream_create");

  GST_DEBUG ("Done linking");
}

static void
urisourcebin_pad_added_cb (GstElement * urisourcebin, GstPad * pad,
    GstDemuxerES * demuxer)
{
  GstDemuxerEStream *stream = NULL;
  GstPad *parsebin_pad;
  GstDemuxerESPrivate *priv = demuxer->priv;

  if (!GST_PAD_IS_SRC (pad))
    return;

  GST_DEBUG ("pad %s:%s", GST_DEBUG_PAD_NAME (pad));

  parsebin_pad = gst_element_get_static_pad (priv->parsebin, "sink");

  if (gst_pad_link (pad, parsebin_pad) != GST_PAD_LINK_OK)
    GST_ERROR ("Unable to link the pad %p to the parsebin pad", pad);
  gst_object_unref (parsebin_pad);

  GST_DEBUG ("Done linking");
}

static void
parsebin_pad_no_more_pads (GstElement * parsebin, GstDemuxerES * demuxer)
{
  GST_INFO ("No more pads received from %s", GST_ELEMENT_NAME (parsebin));
  if (demuxer->priv->state == DEMUXER_ES_STATE_IDLE) {
    set_demuxer_state (demuxer, DEMUXER_ES_STATE_READY);
  }
}

static GstAutoplugSelectResult
parsebin_autoplug_select_cb (GstElement * src, GstPad * pad,
    GstCaps * caps, GstElementFactory * factory, GstDemuxerES * demuxer)
{
  GstAutoplugSelectResult ret = GST_AUTOPLUG_SELECT_TRY;

  if (!factory || gst_element_factory_list_is_type (factory,
          GST_ELEMENT_FACTORY_TYPE_DECODER)) {
    GST_DEBUG ("Expose pad if factory is decoder or null.");
    ret = GST_AUTOPLUG_SELECT_EXPOSE;
  }

  return ret;
}

static gboolean
autoplug_query_caps (GstElement * parsebin, GstPad * pad,
    GstElement * element, GstQuery * query, GstDemuxerES * demuxer)
{
  GstCaps *result = NULL;
  GstElementFactory *factory = gst_element_get_factory (element);

  if (!factory)
    goto done;

  if (gst_element_factory_list_is_type (factory,
          GST_ELEMENT_FACTORY_TYPE_PARSER)) {
    GstCaps *caps = gst_pad_query_caps (pad, NULL);
    if (!caps) {
      GST_ERROR ("Unable to retrive caps from the parser");
      goto done;
    }
    GstStructure *s = gst_caps_get_structure (caps, 0);
    if (!s)
      goto done;
    GstDemuxerESVideoCodec codec_id =
        gst_parse_stream_get_vcodec_from_caps (caps);
    switch (codec_id) {
      case DEMUXER_ES_VIDEO_CODEC_H264:
        result =
            gst_caps_from_string
            ("video/x-h264,stream-format=byte-stream,alignment=au");
        break;
      case DEMUXER_ES_VIDEO_CODEC_H265:
        result =
            gst_caps_from_string
            ("video/x-h265,stream-format=byte-stream,alignment=au");
        break;
      default:
        GST_DEBUG ("Unknown codec id %d", codec_id);
    }

    gst_caps_unref (caps);
    if (result) {
      gst_query_set_caps_result (query, result);
      GST_DEBUG ("the caps is %" GST_PTR_FORMAT, result);
      gst_caps_unref (result);
    }
  }

done:
  return (result != NULL);
}

static gboolean
parsebin_autoplug_query_cb (GstElement * parsebin, GstPad * pad,
    GstElement * element, GstQuery * query, GstDemuxerES * demuxer)
{
  switch (GST_QUERY_TYPE (query)) {
    case GST_QUERY_CAPS:
      return autoplug_query_caps (parsebin, pad, element, query, demuxer);
    default:
      return FALSE;
  }
}

static gboolean
handle_bus_message (GstBus * bus, GstMessage * message, GstDemuxerES * demuxer)
{
  GstDemuxerESPrivate *priv = demuxer->priv;
  switch (GST_MESSAGE_TYPE (message)) {
    case GST_MESSAGE_ERROR:
    {
      set_demuxer_state (demuxer, DEMUXER_ES_STATE_ERROR);
      break;
    }
    case GST_MESSAGE_EOS:
    {
      set_demuxer_state (demuxer, DEMUXER_ES_STATE_EOS);
      break;
    }
    // parsebin does not trigger 'no-more-pads' with elementary stream, rely on stream-collection event instead
    // See https://gitlab.freedesktop.org/gstreamer/gstreamer/-/issues/2119
    case GST_MESSAGE_STREAM_COLLECTION:
    {
      set_demuxer_state (demuxer, DEMUXER_ES_STATE_READY);
      break;
    }
    case GST_MESSAGE_ELEMENT:
    {
      const GstStructure *s;
      const char *sname;
      s = gst_message_get_structure (message);
      sname = gst_structure_get_name (s);
      if ((GST_MESSAGE_SRC (message) == GST_OBJECT (priv->pipeline)) &&
          (!g_strcmp0 (sname, "exit"))) {
        GST_DEBUG ("`exit` message received");
        priv->bus_exit = TRUE;
      }
      break;
    }
    default:
      GST_DEBUG ("message received %s", GST_MESSAGE_TYPE_NAME (message));
      break;
  }

  return TRUE;
}

static gboolean
check_for_bus_message (GstDemuxerES * demuxer)
{
  GstDemuxerESPrivate *priv = demuxer->priv;
  GstBus *bus = gst_pipeline_get_bus (GST_PIPELINE (priv->pipeline));
  gboolean check_message = TRUE;
  while (check_message) {
    GstMessage *message = gst_bus_timed_pop (bus, 1);
    if (G_LIKELY (message)) {
      handle_bus_message (bus, message, demuxer);
      gst_message_unref (message);
    } else
      check_message = FALSE;
  }
  gst_object_unref (bus);

  return TRUE;
}

static gpointer
check_for_bus_message_cb (gpointer data)
{
  GstDemuxerES *demuxer = (GstDemuxerES *) data;
  GstDemuxerESPrivate *priv = demuxer->priv;
  while (!priv->bus_exit) {
    check_for_bus_message (demuxer);
  }
  return NULL;
}

GstDemuxerES *
gst_demuxer_es_new (const gchar * uri)
{
  GstDemuxerES *demuxer;
  GstDemuxerESPrivate *priv;
  GstElement *urisourcebin;
  GstStateChangeReturn sret;
  gchar *current_uri;

  if (!gst_init_check (NULL, NULL, NULL))
    return NULL;

  GST_DEBUG_CATEGORY_INIT (demuxer_es_debug, "demuxeres", 0, "demuxeres");

  demuxer = g_new0 (GstDemuxerES, 1);
  g_assert (demuxer != NULL);
  demuxer->priv = g_new0 (GstDemuxerESPrivate, 1);
  priv = demuxer->priv;

  g_mutex_init (&priv->ready_mutex);
  g_cond_init (&priv->ready_cond);

  current_uri = get_gst_valid_uri (uri);

  priv->pipeline = gst_pipeline_new ("demuxeres");

  urisourcebin = gst_element_factory_make ("urisourcebin", NULL);
  g_object_set (G_OBJECT (urisourcebin), "uri", current_uri, NULL);

  g_signal_connect (urisourcebin, "pad-added",
      G_CALLBACK (urisourcebin_pad_added_cb), demuxer);

  priv->parsebin = gst_element_factory_make ("parsebin", NULL);
  GST_DEBUG ("New demuxeres with uri: %s", current_uri);
  g_free (current_uri);
  g_signal_connect (priv->parsebin, "pad-added",
      G_CALLBACK (parsebin_pad_added_cb), demuxer);
  g_signal_connect (priv->parsebin, "no-more-pads",
      G_CALLBACK (parsebin_pad_no_more_pads), demuxer);
  g_signal_connect (priv->parsebin, "autoplug-select",
      G_CALLBACK (parsebin_autoplug_select_cb), demuxer);
  g_signal_connect (priv->parsebin, "autoplug-query",
      G_CALLBACK (parsebin_autoplug_query_cb), demuxer);

  priv->funnel = gst_element_factory_make ("funnel", "funnel_demuxeres");
  priv->appsink = gst_element_factory_make ("appsink", NULL);
  g_object_set (priv->appsink, "sync", FALSE, NULL);

  gst_bin_add_many (GST_BIN (priv->pipeline), urisourcebin, priv->parsebin,
      priv->funnel, priv->appsink, NULL);

  gst_element_link_many (priv->funnel, priv->appsink, NULL);

  priv->bus_thread =
      g_thread_new ("gst_bus_thread", check_for_bus_message_cb, demuxer);

  sret = gst_element_set_state (priv->pipeline, GST_STATE_PLAYING);
  switch (sret) {
    case GST_STATE_CHANGE_FAILURE:
      /* ignore, we should get an error message posted on the bus */
      GST_ERROR ("Pipeline failed to go to PLAYING state");
      gst_element_set_state (priv->pipeline, GST_STATE_NULL);
      gst_demuxer_es_teardown (demuxer);
      demuxer = NULL;
      break;
    case GST_STATE_CHANGE_NO_PREROLL:
      GST_DEBUG ("Pipeline is live.");
      break;
    case GST_STATE_CHANGE_ASYNC:
      GST_DEBUG ("Prerolling...");
      break;
    default:
      break;
  }
  if (demuxer && !wait_for_demuxer_ready (demuxer)) {
    GST_ERROR ("The demuxer did not get ready state = %d", priv->state);
    gst_demuxer_es_teardown (demuxer);
    demuxer = NULL;
  }
  return demuxer;
}

void
gst_demuxer_es_clear_packet (GstDemuxerESPacket * packet)
{
  GST_LOG ("clear packet: %d", packet->packet_number);
  GstBuffer *buffer = gst_sample_get_buffer (packet->priv->sample);
  gst_buffer_unmap (buffer, &packet->priv->map);
  gst_sample_unref (packet->priv->sample);
  g_free (packet->priv);
  g_free (packet);
}

GstDemuxerESResult
gst_demuxer_es_read_packet (GstDemuxerES * demuxer,
    GstDemuxerESPacket ** packet)
{
  GstDemuxerESPacket *queued_packet;

  GstDemuxerESPrivate *priv = demuxer->priv;
  GstDemuxerESResult result = DEMUXER_ES_RESULT_NO_PACKET;

  check_for_bus_message (demuxer);
  if (priv->state == DEMUXER_ES_STATE_ERROR) {
    return DEMUXER_ES_RESULT_ERROR;
  }

  queued_packet = appsink_read_packet (demuxer);

  if (queued_packet) {
    *packet = queued_packet;
    result = DEMUXER_ES_RESULT_NEW_PACKET;
    if (priv->state == DEMUXER_ES_STATE_EOS) {
      result = DEMUXER_ES_RESULT_LAST_PACKET;
      GST_LOG ("A %s packet of type %d stream_id %d with size %lu.",
          (result == DEMUXER_ES_RESULT_LAST_PACKET) ? "last" : "new",
          queued_packet->stream_type, queued_packet->stream_id,
          queued_packet->data_size);
    }
  }
  return result;
}

GstDemuxerEStream *
gst_demuxer_es_find_best_stream (GstDemuxerES * demuxer,
    GstDemuxerEStreamType type)
{
  GstDemuxerESPrivate *priv;
  GList *l;

  if (!demuxer)
    return NULL;

  priv = demuxer->priv;

  GST_DEBUG_BIN_TO_DOT_FILE_WITH_TS (GST_BIN (priv->pipeline),
      GST_DEBUG_GRAPH_SHOW_ALL, "gst-demuxeres.best_stream");

  if (priv->state == DEMUXER_ES_STATE_IDLE) {
    return NULL;
  }

  for (l = priv->streams; l != NULL; l = g_list_next (l)) {
    GstDemuxerEStream *stream = l->data;
    if (stream->type == type) {
      return stream;
    }
  }

  return NULL;
}

static void
_gst_demuxer_es_cleanup_bus_watch (GstDemuxerES * demuxer)
{
  g_assert (demuxer != NULL);
  GstDemuxerESPrivate *priv = demuxer->priv;
  GstBus *bus = gst_element_get_bus (demuxer->priv->pipeline);
  GThread *bus_thread = priv->bus_thread;
  if (G_LIKELY (bus) && priv->bus_thread) {
    gst_bus_post (bus,
        gst_message_new_element (GST_OBJECT (priv->pipeline),
            gst_structure_new_empty ("exit")));
    GST_LOG ("waiting for message bus thread");
    priv->bus_thread = NULL;
    gst_object_unref (bus);

    g_thread_join (bus_thread);
  }
  priv->bus_exit = TRUE;
}

void
gst_demuxer_es_teardown (GstDemuxerES * demuxer)
{
  GstDemuxerESPrivate *priv = demuxer->priv;
  _gst_demuxer_es_cleanup_bus_watch (demuxer);
  gst_element_set_state (priv->pipeline, GST_STATE_NULL);

  g_list_free_full (priv->streams, (GDestroyNotify) gst_parse_stream_teardown);
  gst_object_unref (priv->pipeline);

  g_cond_clear (&priv->ready_cond);
  g_mutex_clear (&priv->ready_mutex);
  g_free (priv);
  demuxer->priv = NULL;
  g_free (demuxer);
}


const gchar*
gst_demuxer_es_get_stream_type_name(GstDemuxerEStreamType type_id)
{
  switch(type_id) {
    case DEMUXER_ES_STREAM_TYPE_VIDEO:
      return "Video";
    case DEMUXER_ES_STREAM_TYPE_AUDIO:
      return "Audio";
    case DEMUXER_ES_STREAM_TYPE_TEXT:
      return "Text";
    default:
        break;
  }
  return "Unknown";
}

const gchar*
gst_demuxer_es_get_codec_name(GstDemuxerEStreamType type_id, GstDemuxerESVideoCodec codec_id)
{
  if (type_id == DEMUXER_ES_STREAM_TYPE_VIDEO) {
    switch(codec_id) {
      case DEMUXER_ES_VIDEO_CODEC_AV1:
        return "AV1";
      case DEMUXER_ES_VIDEO_CODEC_H264:
        return "H264";
      case DEMUXER_ES_VIDEO_CODEC_H265:
        return "H265";
      case DEMUXER_ES_VIDEO_CODEC_VP9:
        return "VP9";
      default:
        break;
  }

  } else if (type_id == DEMUXER_ES_STREAM_TYPE_AUDIO) {
    switch(codec_id) {
      case DEMUXER_ES_AUDIO_CODEC_AAC:
        return "AAC";
      default:
        break;
    }
  }
  return "Unknown";
}
