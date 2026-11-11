export interface WhatsAppMessage {
  from: string;
  timestamp: string;
  type: 'text' | 'image' | 'document' | 'audio' | 'video' | 'unknown';
  text?: string;
  mediaUrl?: string;
  mediaId?: string;
}

export interface WebhookPayload {
  object?: string;
  entry?: Array<{
    id: string;
    changes: Array<{
      value: {
        messaging_product?: string;
        metadata?: {
          display_phone_number: string;
          phone_number_id: string;
        };
        messages?: Array<{
          from: string;
          id: string;
          timestamp: string;
          type: string;
          text?: { body: string };
          image?: { id: string };
          document?: { id: string };
          audio?: { id: string };
          video?: { id: string };
        }>;
      };
      field: string;
    }>;
  }>;
}

export function extractMessages(payload: WebhookPayload): WhatsAppMessage[] {
  const messages: WhatsAppMessage[] = [];

  if (!payload.entry) {
    return messages;
  }

  for (const entry of payload.entry) {
    if (!entry.changes) {
      continue;
    }

    for (const change of entry.changes) {
      const messages_list = change.value.messages;
      if (!messages_list) {
        continue;
      }

      for (const msg of messages_list) {
        const message: WhatsAppMessage = {
          from: msg.from,
          timestamp: msg.timestamp,
          type: 'unknown',
          text: undefined,
          mediaUrl: undefined,
          mediaId: undefined,
        };

        if (msg.type === 'text' && msg.text) {
          message.type = 'text';
          message.text = msg.text.body;
        } else if (msg.type === 'image' && msg.image) {
          message.type = 'image';
          message.mediaId = msg.image.id;
        } else if (msg.type === 'document' && msg.document) {
          message.type = 'document';
          message.mediaId = msg.document.id;
        } else if (msg.type === 'audio' && msg.audio) {
          message.type = 'audio';
          message.mediaId = msg.audio.id;
        } else if (msg.type === 'video' && msg.video) {
          message.type = 'video';
          message.mediaId = msg.video.id;
        }

        messages.push(message);
      }
    }
  }

  return messages;
}
