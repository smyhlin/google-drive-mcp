import { createHash, randomBytes } from 'node:crypto';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import type { Request, Response } from 'express';
import type { drive_v3 } from 'googleapis';
import { z } from 'zod';
import type { ToolContext, ToolDefinition, ToolResult } from '../types.js';
import { errorResponse } from '../types.js';
import { normalizeArchivePath, parseTarStream, TarFormatError } from '../utils/tarStream.js';

const DEFAULT_TTL_SECONDS = 600;
const MAX_TTL_SECONDS = 3600;
const DEFAULT_MAX_USES = 2;
const MAX_DOWNLOAD_USES = 5;
const UPLOAD_TTL_MS = 10 * 60 * 1000;
const BATCH_UPLOAD_TTL_MS = 15 * 60 * 1000;
const MAX_BATCH_FILES = 100;
const MAX_STAGING_CHUNKS = 100;
const TOKEN_CLEANUP_INTERVAL_MS = 60_000;

type TokenStatus = 'pending' | 'active';

interface UploadTarget {
  name: string;
  mimeType: string;
  parentFolderId: string | undefined;
  fileId: string | undefined;
}

interface BatchFileTarget extends UploadTarget {
  archivePath: string;
  sizeBytes: number | undefined;
}

interface UploadToken extends UploadTarget {
  sizeBytes: number | undefined;
  userId: string | undefined;
  accountAlias: string | undefined;
  expiresAt: number;
  status: TokenStatus;
}

interface BatchUploadToken {
  manifest: Map<string, BatchFileTarget>;
  userId: string | undefined;
  accountAlias: string | undefined;
  expiresAt: number;
  status: TokenStatus;
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

interface UploadedDriveFile {
  id?: string;
  name?: string;
  mimeType?: string;
  size?: string | number;
  webViewLink?: string;
}

export interface BatchUploadedEntry extends UploadedDriveFile {
  archivePath: string;
  mode: 'update_in_place' | 'create_new';
}

export interface BatchFailedEntry {
  archivePath: string;
  name: string;
  error: string;
}

export interface BatchUploadSummary {
  uploaded: BatchUploadedEntry[];
  failed: BatchFailedEntry[];
  missing: string[];
  ignored: string[];
}

const uploadTokens = new Map<string, UploadToken>();
const batchUploadTokens = new Map<string, BatchUploadToken>();
const downloadTokens = new Map<string, DownloadToken>();

const cleanupTimer = setInterval(() => {
  const now = Date.now();
  for (const [token, entry] of uploadTokens) {
    if (entry.expiresAt <= now && entry.status !== 'active') uploadTokens.delete(token);
  }
  for (const [token, entry] of batchUploadTokens) {
    if (entry.expiresAt <= now && entry.status !== 'active') batchUploadTokens.delete(token);
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

const BatchFileEntrySchema = z.object({
  archivePath: z.string().min(1),
  name: z.string().min(1),
  mimeType: z.string().min(1).optional().default('application/octet-stream'),
  parentFolderId: z.string().min(1).optional(),
  fileId: z.string().min(1).optional(),
  sizeBytes: z.number().int().nonnegative().optional(),
});

const BatchUploadUrlSchema = z.object({
  files: z.array(BatchFileEntrySchema).min(1).max(MAX_BATCH_FILES),
});

const CommitStagedUploadSchema = z.object({
  stagingFileIds: z.array(z.string().min(1)).min(1).max(MAX_STAGING_CHUNKS),
  format: z.enum(['raw', 'tar']).optional(),
  name: z.string().min(1).optional(),
  mimeType: z.string().min(1).optional().default('application/octet-stream'),
  parentFolderId: z.string().min(1).optional(),
  fileId: z.string().min(1).optional(),
  files: z.array(BatchFileEntrySchema).min(1).max(MAX_BATCH_FILES).optional(),
  expectedSizeBytes: z.number().int().nonnegative().optional(),
  expectedSha256: z.string().regex(/^[a-fA-F0-9]{64}$/, 'expectedSha256 must be a 64-character hex string').optional(),
  deleteStaging: z.boolean().optional().default(true),
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
    name: 'createBatchUploadUrl',
    description: 'Create a single short-lived upload URL that accepts an uncompressed POSIX PAX/USTAR TAR stream containing multiple files and streams each entry directly to Google Drive in O(1) memory (create new or update in place via fileId). Eliminates multiple curl confirmations.',
    inputSchema: {
      type: 'object',
      properties: {
        files: {
          type: 'array',
          minItems: 1,
          maxItems: MAX_BATCH_FILES,
          description: 'Manifest of files expected inside the TAR stream',
          items: {
            type: 'object',
            properties: {
              archivePath: { type: 'string', description: 'Relative path of the file inside the TAR archive' },
              name: { type: 'string', description: 'Target filename in Google Drive' },
              mimeType: { type: 'string', description: 'MIME type; defaults to application/octet-stream' },
              parentFolderId: { type: 'string', description: 'Destination folder for a new file' },
              fileId: { type: 'string', description: 'Existing Drive file ID to update in place' },
              sizeBytes: { type: 'integer', minimum: 0, description: 'Optional exact byte size assertion for this entry' },
            },
            required: ['archivePath', 'name'],
          },
        },
      },
      required: ['files'],
    },
  },
  {
    name: 'commitStagedUpload',
    description: 'Zero-curl upload bridge: concatenate one or more staged Google Drive chunk files in O(1) memory and commit the reconstructed payload to Drive — either as a single file (format="raw", supporting in-place update via fileId or create via parentFolderId) or by streaming-unpacking a PAX/USTAR TAR archive into multiple targets (format="tar" with files[]). Automatically deletes staging chunks after a clean commit.',
    inputSchema: {
      type: 'object',
      properties: {
        stagingFileIds: {
          type: 'array',
          minItems: 1,
          maxItems: MAX_STAGING_CHUNKS,
          items: { type: 'string' },
          description: 'Ordered list of Google Drive file IDs for the staged file or split chunks',
        },
        format: {
          type: 'string',
          enum: ['raw', 'tar'],
          description: 'Payload format: "raw" for a single target file (default when files[] is omitted) or "tar" for a multi-file TAR archive (default when files[] is provided)',
        },
        name: { type: 'string', description: 'Target filename in Google Drive (required for format="raw")' },
        mimeType: { type: 'string', description: 'MIME type for format="raw"; defaults to application/octet-stream' },
        parentFolderId: { type: 'string', description: 'Destination folder for a new file (format="raw")' },
        fileId: { type: 'string', description: 'Existing Drive file ID to update in place (format="raw")' },
        files: {
          type: 'array',
          minItems: 1,
          maxItems: MAX_BATCH_FILES,
          description: 'Manifest of files to extract and upload when format="tar"',
          items: {
            type: 'object',
            properties: {
              archivePath: { type: 'string', description: 'Relative path of the file inside the TAR archive' },
              name: { type: 'string', description: 'Target filename in Google Drive' },
              mimeType: { type: 'string', description: 'MIME type; defaults to application/octet-stream' },
              parentFolderId: { type: 'string', description: 'Destination folder for a new file' },
              fileId: { type: 'string', description: 'Existing Drive file ID to update in place' },
              sizeBytes: { type: 'integer', minimum: 0, description: 'Optional exact byte size assertion for this entry' },
            },
            required: ['archivePath', 'name'],
          },
        },
        expectedSizeBytes: { type: 'integer', minimum: 0, description: 'Optional exact total byte length across all stagingFileIds; verified before writing' },
        expectedSha256: { type: 'string', description: 'Optional 64-char hex SHA-256 checksum of the reconstructed payload' },
        deleteStaging: { type: 'boolean', default: true, description: 'Permanently delete stagingFileIds after a clean commit (default: true)' },
      },
      required: ['stagingFileIds'],
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

function buildManifest(
  rawFiles: z.infer<typeof BatchFileEntrySchema>[],
): { manifest: Map<string, BatchFileTarget> } | { error: string } {
  const manifest = new Map<string, BatchFileTarget>();
  for (const item of rawFiles) {
    if (item.fileId && item.parentFolderId) {
      return {
        error: `Entry "${item.archivePath}" cannot pass both fileId and parentFolderId. Use fileId to update, or parentFolderId to create a file.`,
      };
    }
    let normalizedPath: string;
    try {
      normalizedPath = normalizeArchivePath(item.archivePath);
    } catch (err) {
      return { error: err instanceof Error ? err.message : String(err) };
    }
    if (manifest.has(normalizedPath)) {
      return { error: `Duplicate archivePath in manifest: "${normalizedPath}"` };
    }
    manifest.set(normalizedPath, {
      archivePath: normalizedPath,
      name: item.name,
      mimeType: item.mimeType,
      parentFolderId: item.parentFolderId,
      fileId: item.fileId,
      sizeBytes: item.sizeBytes,
    });
  }
  return { manifest };
}

async function createGoogleUploadUrl(
  target: UploadTarget,
  accessToken: string,
  contentLength: number,
  fetchImpl: typeof fetch,
): Promise<string> {
  const isUpdate = Boolean(target.fileId);
  const baseUrl = isUpdate
    ? `https://www.googleapis.com/upload/drive/v3/files/${encodeURIComponent(target.fileId!)}`
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
      'X-Upload-Content-Type': target.mimeType,
      'X-Upload-Content-Length': String(contentLength),
    },
    body: JSON.stringify(isUpdate
      ? { name: target.name }
      : { name: target.name, ...(target.parentFolderId ? { parents: [target.parentFolderId] } : {}) }),
  });
  if (!response.ok) {
    throw new Error(`Google Drive resumable init failed (${response.status}): ${await response.text()}`);
  }
  const uploadUrl = response.headers.get('location');
  if (!uploadUrl) throw new Error('Google Drive did not return a Location header');
  return uploadUrl;
}

async function uploadStreamToGoogleDrive(
  target: UploadTarget,
  accessToken: string,
  contentLength: number,
  bodyStream: Readable,
  fetchImpl: typeof fetch,
): Promise<{ ok: boolean; status: number; responseText: string }> {
  const googleUploadUrl = await createGoogleUploadUrl(target, accessToken, contentLength, fetchImpl);
  const uploadResponse = await fetchImpl(googleUploadUrl, {
    method: 'PUT',
    headers: {
      'Content-Type': target.mimeType,
      'Content-Length': String(contentLength),
    },
    body: Readable.toWeb(bodyStream) as ReadableStream<Uint8Array>,
    duplex: 'half',
  } as RequestInit & { duplex: 'half' });
  const responseText = await uploadResponse.text();
  return { ok: uploadResponse.ok, status: uploadResponse.status, responseText };
}

function parseDriveJson(responseText: string): UploadedDriveFile {
  try {
    const parsed = JSON.parse(responseText) as UploadedDriveFile;
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

async function processTarManifestToDrive(
  source: AsyncIterable<Buffer | Uint8Array>,
  manifest: Map<string, BatchFileTarget>,
  accessToken: string,
  fetchImpl: typeof fetch,
): Promise<BatchUploadSummary> {
  const uploaded: BatchUploadedEntry[] = [];
  const failed: BatchFailedEntry[] = [];
  const ignored: string[] = [];
  const seen = new Set<string>();

  await parseTarStream(source, async ({ header, stream }) => {
    const target = manifest.get(header.path);
    if (!target) {
      ignored.push(header.path);
      stream.resume();
      return;
    }
    if (seen.has(header.path)) {
      failed.push({
        archivePath: header.path,
        name: target.name,
        error: `Duplicate entry for "${header.path}" in TAR archive`,
      });
      stream.resume();
      return;
    }
    seen.add(header.path);

    if (target.sizeBytes !== undefined && target.sizeBytes !== header.size) {
      failed.push({
        archivePath: header.path,
        name: target.name,
        error: `Entry size (${header.size}) does not match expected sizeBytes (${target.sizeBytes})`,
      });
      stream.resume();
      return;
    }

    try {
      const result = await uploadStreamToGoogleDrive(target, accessToken, header.size, stream, fetchImpl);
      if (!result.ok) {
        failed.push({
          archivePath: header.path,
          name: target.name,
          error: `Google Drive rejected upload (${result.status}): ${result.responseText}`,
        });
        return;
      }
      const driveFile = parseDriveJson(result.responseText);
      uploaded.push({
        ...driveFile,
        archivePath: header.path,
        mode: target.fileId ? 'update_in_place' : 'create_new',
      });
    } catch (err) {
      stream.resume();
      failed.push({
        archivePath: header.path,
        name: target.name,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  const missing = [...manifest.keys()].filter((path) => !seen.has(path));
  return { uploaded, failed, missing, ignored };
}

function concatDriveStreams(
  drive: drive_v3.Drive,
  stagingFileIds: string[],
  hash: ReturnType<typeof createHash>,
): Readable {
  async function* generate(): AsyncGenerator<Buffer> {
    for (const fileId of stagingFileIds) {
      const response = await drive.files.get(
        { fileId, alt: 'media', supportsAllDrives: true },
        { responseType: 'stream' },
      );
      const stream = response.data as unknown as AsyncIterable<Buffer | Uint8Array>;
      for await (const chunk of stream) {
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        if (buf.length > 0) {
          hash.update(buf);
          yield buf;
        }
      }
    }
  }
  return Readable.from(generate());
}

async function cleanupStagingFiles(
  drive: drive_v3.Drive,
  stagingFileIds: string[],
): Promise<{ deleted: string[]; failed: Array<{ fileId: string; error: string }> }> {
  const deleted: string[] = [];
  const failed: Array<{ fileId: string; error: string }> = [];
  const results = await Promise.allSettled(
    stagingFileIds.map(async (fileId) => {
      await drive.files.delete({ fileId, supportsAllDrives: true });
      return fileId;
    }),
  );
  results.forEach((outcome, index) => {
    const fileId = stagingFileIds[index];
    if (outcome.status === 'fulfilled') {
      deleted.push(fileId);
    } else {
      failed.push({
        fileId,
        error: outcome.reason instanceof Error ? outcome.reason.message : String(outcome.reason),
      });
    }
  });
  return { deleted, failed };
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
    if (token && entry?.status !== 'active') uploadTokens.delete(token);
    res.status(404).json({ error: 'Upload token expired or invalid' });
    return;
  }
  if (entry.status === 'active') {
    res.status(409).json({ error: 'Upload token is already in use' });
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

  entry.status = 'active';
  try {
    const authClient = await getAuthClient({ userId: entry.userId, accountAlias: entry.accountAlias });
    const accessToken = await getAccessToken(authClient);
    const result = await uploadStreamToGoogleDrive(entry, accessToken, contentLength, req, fetchImpl);
    if (!result.ok) {
      if (entry.expiresAt > Date.now()) entry.status = 'pending';
      else uploadTokens.delete(token);
      res.status(502).json({ error: 'Google Drive rejected the upload', details: result.responseText });
      return;
    }
    uploadTokens.delete(token);
    res.status(result.status).type('application/json').send(result.responseText);
  } catch (error) {
    if (entry.expiresAt > Date.now()) entry.status = 'pending';
    else uploadTokens.delete(token);
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

export async function streamBatchUpload(
  req: Request,
  res: Response,
  getAuthClient: (owner: UploadOwner) => Promise<{ getAccessToken(): Promise<string | { token?: string | null }> }>,
  fetchImpl: typeof fetch = fetch,
): Promise<void> {
  const tokenParam = req.params.token;
  const token = Array.isArray(tokenParam) ? tokenParam[0] : tokenParam;
  const entry = token ? batchUploadTokens.get(token) : undefined;
  if (!token || !entry || entry.expiresAt <= Date.now()) {
    if (token && entry?.status !== 'active') batchUploadTokens.delete(token);
    res.status(404).json({ error: 'Batch upload token expired or invalid' });
    return;
  }
  if (entry.status === 'active') {
    res.status(409).json({ error: 'Batch upload token is already in use' });
    return;
  }

  entry.status = 'active';
  try {
    const authClient = await getAuthClient({ userId: entry.userId, accountAlias: entry.accountAlias });
    const accessToken = await getAccessToken(authClient);
    const summary = await processTarManifestToDrive(req, entry.manifest, accessToken, fetchImpl);
    batchUploadTokens.delete(token);
    const hasIssues = summary.failed.length > 0 || summary.missing.length > 0;
    res.status(hasIssues ? 207 : 200).json(summary);
  } catch (error) {
    if (error instanceof TarFormatError) {
      if (entry.expiresAt > Date.now()) entry.status = 'pending';
      else batchUploadTokens.delete(token);
      if (!res.headersSent) {
        res.status(400).json({ error: error.message });
      }
      return;
    }
    if (entry.expiresAt > Date.now()) entry.status = 'pending';
    else batchUploadTokens.delete(token);
    if (res.headersSent || res.destroyed) {
      res.destroy(error instanceof Error ? error : new Error(String(error)));
      return;
    }
    res.status(502).json({
      error: 'Failed to stream batch upload to Google Drive',
      details: error instanceof Error ? error.message : String(error),
    });
  }
}

function shellQuote(value: string): string {
  return `"${value.replace(/(["\\$`])/g, '\\$1')}"`;
}

export async function handleTool(
  toolName: string,
  args: Record<string, unknown>,
  ctx: ToolContext,
  fetchImpl: typeof fetch = fetch,
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
        status: 'pending',
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

  if (toolName === 'createBatchUploadUrl') {
    const parsed = BatchUploadUrlSchema.safeParse(args);
    if (!parsed.success) return errorResponse(parsed.error.issues[0]?.message ?? 'Invalid batch upload URL arguments');
    if (!ctx.uploadEndpointAvailable) {
      return errorResponse('createBatchUploadUrl requires the Streamable HTTP transport.');
    }
    const built = buildManifest(parsed.data.files);
    if ('error' in built) return errorResponse(built.error);

    try {
      const base = publicBaseUrl();
      const token = randomBytes(32).toString('hex');
      batchUploadTokens.set(token, {
        manifest: built.manifest,
        userId: ctx.userId,
        accountAlias: ctx.accountAlias,
        expiresAt: Date.now() + BATCH_UPLOAD_TTL_MS,
        status: 'pending',
      });
      const uploadUrl = new URL(`/upload/batch/${token}`, base).href;
      const pathsArg = [...built.manifest.keys()].map(shellQuote).join(' ');
      return resultText({
        mode: 'batch_tar',
        fileCount: built.manifest.size,
        uploadUrl,
        expiresIn: '15 minutes',
        files: [...built.manifest.values()].map((entry) => ({
          archivePath: entry.archivePath,
          name: entry.name,
          mimeType: entry.mimeType,
          mode: entry.fileId ? 'update_in_place' : 'create_new',
          fileId: entry.fileId ?? null,
          parentFolderId: entry.parentFolderId ?? null,
        })),
        tarCurlExample: `tar --format=pax -cf - ${pathsArg} | curl -fSL -X PUT --data-binary @- "${uploadUrl}"`,
      });
    } catch (error) {
      return errorResponse(error instanceof Error ? error.message : String(error));
    }
  }

  if (toolName === 'commitStagedUpload') {
    const parsed = CommitStagedUploadSchema.safeParse(args);
    if (!parsed.success) return errorResponse(parsed.error.issues[0]?.message ?? 'Invalid staged upload arguments');
    const data = parsed.data;
    const effectiveFormat = data.format ?? (data.files ? 'tar' : 'raw');

    if (effectiveFormat === 'raw') {
      if (data.files) {
        return errorResponse('Do not pass files[] when format is "raw". Use format="tar" for multi-file archives.');
      }
      if (!data.name) {
        return errorResponse('name is required when format is "raw".');
      }
      if (data.fileId && data.parentFolderId) {
        return errorResponse('Do not pass both fileId and parentFolderId. Use fileId to update, or parentFolderId to create a file.');
      }
    } else {
      if (!data.files || data.files.length === 0) {
        return errorResponse('files[] manifest is required when format is "tar".');
      }
      if (data.fileId || data.parentFolderId || data.name) {
        return errorResponse('Do not pass top-level name, fileId, or parentFolderId when format is "tar"; specify targets inside files[].');
      }
    }

    let manifest: Map<string, BatchFileTarget> | undefined;
    if (effectiveFormat === 'tar') {
      const built = buildManifest(data.files!);
      if ('error' in built) return errorResponse(built.error);
      manifest = built.manifest;
    }

    try {
      const drive = ctx.getDrive();
      const chunkMetas = await Promise.all(
        data.stagingFileIds.map(async (stagingId) => {
          const meta = await drive.files.get({
            fileId: stagingId,
            fields: 'id,name,size,trashed',
            supportsAllDrives: true,
          });
          if (meta.data.trashed) {
            throw new Error(`Staging file ${stagingId} is in Trash`);
          }
          if (meta.data.size === undefined || meta.data.size === null || !/^[0-9]+$/.test(String(meta.data.size))) {
            throw new Error(`Staging file ${stagingId} has no readable byte size (Google Workspace docs cannot be staged chunks)`);
          }
          const size = Number(meta.data.size);
          if (!Number.isSafeInteger(size) || size < 0) {
            throw new Error(`Staging file ${stagingId} size is out of safe integer range`);
          }
          return { id: meta.data.id ?? stagingId, size };
        }),
      );

      const totalSize = chunkMetas.reduce((sum, item) => sum + item.size, 0);
      if (data.expectedSizeBytes !== undefined && totalSize !== data.expectedSizeBytes) {
        return errorResponse(
          `Staged total size (${totalSize} bytes across ${chunkMetas.length} chunk(s)) does not match expectedSizeBytes (${data.expectedSizeBytes}). Target file(s) were not modified.`,
        );
      }

      const accessToken = await getAccessToken(ctx.authClient);
      const hash = createHash('sha256');
      const combinedStream = concatDriveStreams(drive, data.stagingFileIds, hash);

      if (effectiveFormat === 'raw') {
        const target: UploadTarget = {
          name: data.name!,
          mimeType: data.mimeType,
          parentFolderId: data.parentFolderId,
          fileId: data.fileId,
        };
        const uploadResult = await uploadStreamToGoogleDrive(target, accessToken, totalSize, combinedStream, fetchImpl);
        if (!uploadResult.ok) {
          return errorResponse(`Google Drive rejected staged commit (${uploadResult.status}): ${uploadResult.responseText}`);
        }
        const actualSha256 = hash.digest('hex');
        const sha256Mismatch = Boolean(
          data.expectedSha256 && actualSha256.toLowerCase() !== data.expectedSha256.toLowerCase(),
        );
        const driveFile = parseDriveJson(uploadResult.responseText);

        if (sha256Mismatch) {
          return {
            isError: true,
            content: [{
              type: 'text',
              text: JSON.stringify({
                error: 'SHA-256 mismatch after streaming staged chunks; staging files were preserved for inspection.',
                expectedSha256: data.expectedSha256!.toLowerCase(),
                actualSha256,
                file: driveFile,
              }, null, 2),
            }],
          };
        }

        const stagingCleanup = data.deleteStaging
          ? await cleanupStagingFiles(drive, data.stagingFileIds)
          : { deleted: [], failed: [] };

        return resultText({
          format: 'raw',
          mode: target.fileId ? 'update_in_place' : 'create_new',
          chunkCount: data.stagingFileIds.length,
          totalSizeBytes: totalSize,
          sha256: actualSha256,
          file: driveFile,
          stagingCleanup,
        });
      }

      // format === 'tar'
      const summary = await processTarManifestToDrive(combinedStream, manifest!, accessToken, fetchImpl);
      // Drain any trailing bytes (e.g. post-EOF padding) so the SHA-256 covers the full staged stream
      for await (const chunk of combinedStream) {
        void chunk;
      }
      const actualSha256 = hash.digest('hex');
      const sha256Mismatch = Boolean(
        data.expectedSha256 && actualSha256.toLowerCase() !== data.expectedSha256.toLowerCase(),
      );
      const cleanCommit = summary.failed.length === 0 && summary.missing.length === 0 && !sha256Mismatch;

      const stagingCleanup = data.deleteStaging && cleanCommit
        ? await cleanupStagingFiles(drive, data.stagingFileIds)
        : { deleted: [], failed: [] };

      const payload = {
        format: 'tar',
        chunkCount: data.stagingFileIds.length,
        totalSizeBytes: totalSize,
        sha256: actualSha256,
        ...(sha256Mismatch ? { sha256Mismatch: true, expectedSha256: data.expectedSha256!.toLowerCase() } : {}),
        ...summary,
        stagingCleanup,
      };

      if (!cleanCommit) {
        return {
          isError: true,
          content: [{ type: 'text', text: JSON.stringify(payload, null, 2) }],
        };
      }
      return resultText(payload);
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
