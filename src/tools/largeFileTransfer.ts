import { randomBytes } from 'node:crypto';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import type { Request, Response } from 'express';
import type { drive_v3 } from 'googleapis';
import { z } from 'zod';
import type { ToolContext, ToolDefinition, ToolResult } from '../types.js';
import { errorResponse } from '../types.js';

const DEFAULT_TTL_SECONDS = 600;
const MAX_TTL_SECONDS = 3600;
const DEFAULT_MAX_USES = 2;
const MAX_DOWNLOAD_USES = 5;
const UPLOAD_TTL_MS = 10 * 60 * 1000;
const TOKEN_CLEANUP_INTERVAL_MS = 60_000;

interface UploadToken {
  name: string;
  mimeType: string;
  parentFolderId: string | undefined;
  fileId: string | undefined;
  sizeBytes: number | undefined;
  userId: string | undefined;
  accountAlias: string | undefined;
  expiresAt: number;
}

interface DownloadToken {
  fileId: string;
  name: string;
  mimeType: string;
  size: number | null;
  userId: string | undefined;
  accountAlias: string | undefined;
  expiresAt: number;
  usesLeft: number;
}

const uploadTokens = new Map<string, UploadToken>();
const downloadTokens = new Map<string, DownloadToken>();

const cleanupTimer = setInterval(() => {
  const now = Date.now();
  for (const [token, entry] of uploadTokens) {
    if (entry.expiresAt <= now) uploadTokens.delete(token);
  }
  for (const [token, entry] of downloadTokens) {
    if (entry.expiresAt <= now || entry.usesLeft <= 0) downloadTokens.delete(token);
  }
}, TOKEN_CLEANUP_INTERVAL_MS);
cleanupTimer.unref();

const UploadUrlSchema = z.object({
  name: z.string().min(1),
  mimeType: z.string().min(1).optional().default('application/octet-stream'),
  parentFolderId: z.string().min(1).optional(),
  fileId: z.string().min(1).optional(),
  sizeBytes: z.number().int().positive().optional(),
});

const DownloadUrlSchema = z.object({
  fileId: z.string().min(1),
  ttlSeconds: z.number().int().min(60).max(MAX_TTL_SECONDS).optional().default(DEFAULT_TTL_SECONDS),
  maxUses: z.number().int().min(1).max(MAX_DOWNLOAD_USES).optional().default(DEFAULT_MAX_USES),
});

export const toolDefinitions: ToolDefinition[] = [
  {
    name: 'createUploadUrl',
    description: 'Create a short-lived upload URL on this HTTP server. PUT the file to the returned URL with curl; the server streams it to Google Drive. Pass fileId to update an existing file in place.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Target filename in Google Drive' },
        mimeType: { type: 'string', description: 'MIME type; defaults to application/octet-stream' },
        parentFolderId: { type: 'string', description: 'Destination folder for a new file' },
        fileId: { type: 'string', description: 'Existing Drive file ID to update in place' },
        sizeBytes: { type: 'integer', minimum: 1, description: 'Exact upload size in bytes' },
      },
      required: ['name'],
    },
  },
  {
    name: 'createDownloadUrl',
    description: 'Create a short-lived, limited-use HTTPS URL that streams a Drive file to curl without buffering it in memory.',
    inputSchema: {
      type: 'object',
      properties: {
        fileId: { type: 'string', description: 'Google Drive file ID' },
        ttlSeconds: { type: 'integer', minimum: 60, maximum: MAX_TTL_SECONDS, default: DEFAULT_TTL_SECONDS },
        maxUses: { type: 'integer', minimum: 1, maximum: MAX_DOWNLOAD_USES, default: DEFAULT_MAX_USES },
      },
      required: ['fileId'],
    },
  },
];

function resultText(value: unknown): ToolResult {
  return { content: [{ type: 'text', text: JSON.stringify(value, null, 2) }] };
}

function getAccessToken(authClient: { getAccessToken(): Promise<string | { token?: string | null }> }): Promise<string> {
  return authClient.getAccessToken().then((response) => {
    const token = typeof response === 'string' ? response : response?.token;
    if (!token) throw new Error('Failed to obtain Google OAuth access token');
    return token;
  });
}

function publicBaseUrl(): URL {
  const raw = process.env.PUBLIC_BASE_URL;
  if (!raw) throw new Error('PUBLIC_BASE_URL is not configured on the MCP server.');
  const base = new URL(raw);
  if (base.protocol !== 'https:' || base.pathname !== '/' || base.search || base.hash) {
    throw new Error('PUBLIC_BASE_URL must be an HTTPS origin without a path, query, or fragment.');
  }
  return base;
}

async function createGoogleUploadUrl(
  session: UploadToken,
  accessToken: string,
  contentLength: number,
  fetchImpl: typeof fetch,
): Promise<string> {
  const isUpdate = Boolean(session.fileId);
  const baseUrl = isUpdate
    ? `https://www.googleapis.com/upload/drive/v3/files/${encodeURIComponent(session.fileId!)}`
    : 'https://www.googleapis.com/upload/drive/v3/files';
  const endpoint = new URL(baseUrl);
  endpoint.searchParams.set('uploadType', 'resumable');
  endpoint.searchParams.set('supportsAllDrives', 'true');
  endpoint.searchParams.set('fields', 'id,name,mimeType,size,webViewLink');

  const response = await fetchImpl(endpoint, {
    method: isUpdate ? 'PATCH' : 'POST',
    headers: {
      Authorization: `Bearer ${accessToken}`,
      'Content-Type': 'application/json; charset=UTF-8',
      'X-Upload-Content-Type': session.mimeType,
      'X-Upload-Content-Length': String(contentLength),
    },
    body: JSON.stringify(isUpdate
      ? { name: session.name }
      : { name: session.name, ...(session.parentFolderId ? { parents: [session.parentFolderId] } : {}) }),
  });
  if (!response.ok) {
    throw new Error(`Google Drive resumable init failed (${response.status}): ${await response.text()}`);
  }
  const uploadUrl = response.headers.get('location');
  if (!uploadUrl) throw new Error('Google Drive did not return a Location header');
  return uploadUrl;
}

export interface UploadOwner {
  userId: string | undefined;
  accountAlias: string | undefined;
}

export async function streamUpload(
  req: Request,
  res: Response,
  getAuthClient: (owner: UploadOwner) => Promise<{ getAccessToken(): Promise<string | { token?: string | null }> }>,
  fetchImpl: typeof fetch = fetch,
): Promise<void> {
  const tokenParam = req.params.token;
  const token = Array.isArray(tokenParam) ? tokenParam[0] : tokenParam;
  const entry = token ? uploadTokens.get(token) : undefined;
  if (!token || !entry || entry.expiresAt <= Date.now()) {
    if (token) uploadTokens.delete(token);
    res.status(404).json({ error: 'Upload token expired or invalid' });
    return;
  }

  const lengthHeader = req.headers['content-length'];
  if (Array.isArray(lengthHeader)) {
    res.status(400).json({ error: 'Invalid Content-Length' });
    return;
  }
  if (lengthHeader !== undefined && (!/^[0-9]+$/.test(lengthHeader) || !Number.isSafeInteger(Number(lengthHeader)))) {
    res.status(400).json({ error: 'Invalid Content-Length' });
    return;
  }
  const contentLength = lengthHeader === undefined ? entry.sizeBytes : Number(lengthHeader);
  if (contentLength === undefined) {
    res.status(411).json({ error: 'Content-Length is required for resumable uploads' });
    return;
  }
  if (!Number.isSafeInteger(contentLength)) {
    res.status(400).json({ error: 'Content-Length must be a safe integer' });
    return;
  }
  if (entry.sizeBytes !== undefined && entry.sizeBytes !== contentLength) {
    res.status(400).json({ error: 'Content-Length does not match sizeBytes' });
    return;
  }

  uploadTokens.delete(token);
  try {
    const authClient = await getAuthClient({ userId: entry.userId, accountAlias: entry.accountAlias });
    const accessToken = await getAccessToken(authClient);
    const googleUploadUrl = await createGoogleUploadUrl(entry, accessToken, contentLength, fetchImpl);
    const uploadResponse = await fetchImpl(googleUploadUrl, {
      method: 'PUT',
      headers: {
        'Content-Type': entry.mimeType,
        'Content-Length': String(contentLength),
      },
      body: Readable.toWeb(req) as ReadableStream<Uint8Array>,
      duplex: 'half',
    } as RequestInit & { duplex: 'half' });
    const responseText = await uploadResponse.text();
    if (!uploadResponse.ok) {
      res.status(502).json({ error: 'Google Drive rejected the upload', details: responseText });
      return;
    }
    res.status(uploadResponse.status).type('application/json').send(responseText);
  } catch (error) {
    if (res.headersSent || res.destroyed) {
      res.destroy(error instanceof Error ? error : new Error(String(error)));
      return;
    }
    res.status(502).json({
      error: 'Failed to stream upload to Google Drive',
      details: error instanceof Error ? error.message : String(error),
    });
  }
}

export async function handleTool(
  toolName: string,
  args: Record<string, unknown>,
  ctx: ToolContext,
): Promise<ToolResult | null> {
  if (toolName === 'createUploadUrl') {
    const parsed = UploadUrlSchema.safeParse(args);
    if (!parsed.success) return errorResponse(parsed.error.issues[0]?.message ?? 'Invalid upload URL arguments');
    if (!ctx.uploadEndpointAvailable) {
      return errorResponse('createUploadUrl requires the Streamable HTTP transport.');
    }
    if (parsed.data.fileId && parsed.data.parentFolderId) {
      return errorResponse('Do not pass both fileId and parentFolderId. Use fileId to update, or parentFolderId to create a file.');
    }
    try {
      const base = publicBaseUrl();
      const token = randomBytes(32).toString('hex');
      uploadTokens.set(token, {
        name: parsed.data.name,
        mimeType: parsed.data.mimeType,
        parentFolderId: parsed.data.parentFolderId,
        fileId: parsed.data.fileId,
        sizeBytes: parsed.data.sizeBytes,
        userId: ctx.userId,
        accountAlias: ctx.accountAlias,
        expiresAt: Date.now() + UPLOAD_TTL_MS,
      });
      const uploadUrl = new URL(`/upload/${token}`, base).href;
      return resultText({
        mode: parsed.data.fileId ? 'update_in_place' : 'create_new',
        name: parsed.data.name,
        mimeType: parsed.data.mimeType,
        parentFolderId: parsed.data.parentFolderId ?? null,
        fileId: parsed.data.fileId ?? null,
        uploadUrl,
        expiresIn: '10 minutes',
        curlExample: `curl -fSL -T "${parsed.data.name}" "${uploadUrl}"`,
      });
    } catch (error) {
      return errorResponse(error instanceof Error ? error.message : String(error));
    }
  }

  if (toolName === 'createDownloadUrl') {
    const parsed = DownloadUrlSchema.safeParse(args);
    if (!parsed.success) return errorResponse(parsed.error.issues[0]?.message ?? 'Invalid download URL arguments');
    try {
      const base = publicBaseUrl();
      const metadata = await ctx.getDrive().files.get({
        fileId: parsed.data.fileId,
        fields: 'id,name,mimeType,size',
        supportsAllDrives: true,
      });
      const token = randomBytes(32).toString('hex');
      downloadTokens.set(token, {
        fileId: metadata.data.id ?? parsed.data.fileId,
        name: metadata.data.name ?? `${parsed.data.fileId}.bin`,
        mimeType: metadata.data.mimeType ?? 'application/octet-stream',
        size: metadata.data.size !== undefined && metadata.data.size !== null
          ? Number(metadata.data.size)
          : null,
        userId: ctx.userId,
        accountAlias: ctx.accountAlias,
        expiresAt: Date.now() + parsed.data.ttlSeconds * 1000,
        usesLeft: parsed.data.maxUses,
      });
      const downloadUrl = new URL(`/dl/${token}`, base).href;
      return resultText({
        fileId: metadata.data.id ?? parsed.data.fileId,
        name: metadata.data.name,
        mimeType: metadata.data.mimeType,
        sizeBytes: metadata.data.size ? Number(metadata.data.size) : null,
        downloadUrl,
        expiresInSeconds: parsed.data.ttlSeconds,
        maxUses: parsed.data.maxUses,
        curlExample: `curl -fSL "${downloadUrl}" -o "${metadata.data.name ?? `${parsed.data.fileId}.bin`}"`,
      });
    } catch (error) {
      return errorResponse(error instanceof Error ? error.message : String(error));
    }
  }
  return null;
}

export async function streamDownload(
  req: Request,
  res: Response,
  getDrive: (owner: { userId: string | undefined; accountAlias: string | undefined }) => Promise<drive_v3.Drive>,
): Promise<void> {
  const tokenParam = req.params.token;
  const token = Array.isArray(tokenParam) ? tokenParam[0] : tokenParam;
  const entry = token ? downloadTokens.get(token) : undefined;
  if (!token || !entry || entry.expiresAt <= Date.now() || entry.usesLeft <= 0) {
    if (token) downloadTokens.delete(token);
    res.status(404).json({ error: 'Download token expired or invalid' });
    return;
  }

  entry.usesLeft -= 1;
  if (entry.usesLeft === 0) downloadTokens.delete(token);

  try {
    const drive = await getDrive({ userId: entry.userId, accountAlias: entry.accountAlias });
    const driveResponse = await drive.files.get(
      { fileId: entry.fileId, alt: 'media', supportsAllDrives: true },
      { responseType: 'stream' },
    );
    res.setHeader('Content-Type', entry.mimeType);
    res.setHeader('Content-Disposition', `attachment; filename*=UTF-8''${encodeURIComponent(entry.name)}`);
    if (entry.size !== null) res.setHeader('Content-Length', String(entry.size));
    res.setHeader('Cache-Control', 'no-store');
    await pipeline(driveResponse.data as unknown as Readable, res);
  } catch (error) {
    if (res.headersSent) {
      res.destroy(error instanceof Error ? error : new Error(String(error)));
    } else {
      res.status(502).json({ error: 'Failed to stream file from Google Drive' });
    }
  }
}

export function downloadTokenCount(): number {
  return downloadTokens.size;
}
