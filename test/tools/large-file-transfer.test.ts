import assert from 'node:assert/strict';
import { Readable, Writable } from 'node:stream';
import test from 'node:test';
import type { Request, Response } from 'express';
import type { ToolContext } from '../../src/types.js';
import { handleTool, streamDownload, streamUpload } from '../../src/tools/largeFileTransfer.js';

const baseInput = {
  name: 'archive.zip',
  mimeType: 'application/zip',
  sizeBytes: 1024,
};

test('upload capability streams create and update bodies through the bound Google account', async () => {
  const oldBaseUrl = process.env.PUBLIC_BASE_URL;
  process.env.PUBLIC_BASE_URL = 'https://drive.example:8443';
  try {
    const data = Buffer.from('streamed-file-body');
    const calls: Array<{ url: URL; init: RequestInit }> = [];
    const uploadedBodies: Buffer[] = [];
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input));
      const request = init ?? {};
      calls.push({ url, init: request });
      if (url.hostname === 'www.googleapis.com') {
        return new Response(null, { status: 200, headers: { location: 'https://upload.example/session' } });
      }
      uploadedBodies.push(Buffer.from(await new Response(request.body as ReadableStream).arrayBuffer()));
      return new Response(JSON.stringify({ id: 'file-id', name: 'archive.zip' }), { status: 200 });
    }) as typeof fetch;
    const authClient = { getAccessToken: async () => ({ token: 'access-token' }) };
    const context = {
      userId: 'google-sub-1',
      uploadEndpointAvailable: true,
      accountAlias: 'work',
      getDrive: () => {
        throw new Error('Drive metadata should not be accessed while creating an upload URL');
      },
    } as unknown as ToolContext;

    const stdioResult = await handleTool('createUploadUrl', baseInput, {
      ...context,
      uploadEndpointAvailable: false,
    });
    assert.equal(stdioResult?.isError, true);

    async function upload(input: Record<string, unknown>) {
      const result = await handleTool('createUploadUrl', { ...input, sizeBytes: data.length }, context);
      assert.ok(result);
      const uploadUrl = new URL(JSON.parse(result.content[0].text ?? '{}').uploadUrl);
      assert.equal(uploadUrl.origin, 'https://drive.example:8443');
      assert.match(uploadUrl.pathname, /^\/upload\/[a-f0-9]{64}$/);
      const request = Object.assign(Readable.from([data]), {
        params: { token: uploadUrl.pathname.split('/').at(-1)! },
        headers: { 'content-length': String(data.length) },
      }) as unknown as Request;
      const response = new TestResponse();
      const owners: Array<{ userId: string | undefined; accountAlias: string | undefined }> = [];
      await streamUpload(request, response as unknown as Response, async (owner) => {
        owners.push(owner);
        return authClient;
      }, fetchImpl);
      assert.equal(response.statusCode, 200);
      assert.deepEqual(owners, [{ userId: 'google-sub-1', accountAlias: 'work' }]);
      assert.deepEqual(JSON.parse(Buffer.concat(response.chunks).toString()), { id: 'file-id', name: 'archive.zip' });
      return uploadUrl.pathname.split('/').at(-1)!;
    }

    const createToken = await upload({ ...baseInput, parentFolderId: 'folder-1' });
    await upload({ ...baseInput, fileId: 'file-1' });

    assert.equal(calls[0].init.method, 'POST');
    assert.equal(calls[0].url.searchParams.get('uploadType'), 'resumable');
    assert.equal((calls[0].init.headers as Record<string, string>).Authorization, 'Bearer access-token');
    assert.equal((calls[0].init.headers as Record<string, string>)['X-Upload-Content-Length'], String(data.length));
    assert.deepEqual(JSON.parse(String(calls[0].init.body)), { name: 'archive.zip', parents: ['folder-1'] });
    assert.equal(calls[1].init.method, 'PUT');
    assert.equal((calls[1].init.headers as Record<string, string>).Authorization, undefined);
    assert.equal(calls[2].init.method, 'PATCH');
    assert.match(calls[2].url.pathname, /\/files\/file-1$/);
    assert.deepEqual(JSON.parse(String(calls[2].init.body)), { name: 'archive.zip' });
    assert.deepEqual(uploadedBodies, [data, data]);

    const replayResponse = new TestResponse();
    await streamUpload(
      Object.assign(Readable.from([]), {
        params: { token: createToken },
        headers: { 'content-length': '0' },
      }) as unknown as Request,
      replayResponse as unknown as Response,
      async () => authClient,
      fetchImpl,
    );
    assert.equal(replayResponse.statusCode, 404);
  } finally {
    if (oldBaseUrl === undefined) delete process.env.PUBLIC_BASE_URL;
    else process.env.PUBLIC_BASE_URL = oldBaseUrl;
  }
});

class TestResponse extends Writable {
  headers: Record<string, string> = {};
  chunks: Buffer[] = [];
  statusCode = 200;
  headersSent = false;
  body: unknown;

  setHeader(name: string, value: string): this {
    this.headers[name] = value;
    this.headersSent = true;
    return this;
  }

  status(code: number): this {
    this.statusCode = code;
    return this;
  }

  json(body: unknown): this {
    this.body = body;
    this.end();
    return this;
  }

  type(_contentType: string): this {
    return this;
  }

  send(body: string): this {
    this.end(body);
    return this;
  }

  override _write(chunk: Buffer, _encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
    this.chunks.push(Buffer.from(chunk));
    callback();
  }
}

test('download capability streams the correct user file and expires after its use limit', async () => {
  const oldBaseUrl = process.env.PUBLIC_BASE_URL;
  process.env.PUBLIC_BASE_URL = 'https://drive.example:8443';
  try {
    const data = Buffer.from('large-file-stream');
    const drive = {
      files: {
        get: async (params: { alt?: string }) => params.alt === 'media'
          ? { data: Readable.from([data]) }
          : { data: { id: 'file-1', name: 'sample.bin', mimeType: 'application/octet-stream', size: String(data.length) } },
      },
    };
    const result = await handleTool('createDownloadUrl', { fileId: 'file-1', maxUses: 1 }, {
      userId: 'google-sub-1',
      getDrive: () => drive,
    } as unknown as ToolContext);
    assert.ok(result);
    const payload = JSON.parse(result.content[0].text ?? '{}');
    const download = new URL(payload.downloadUrl);
    assert.equal(download.origin, 'https://drive.example:8443');
    assert.equal(download.pathname.split('/').filter(Boolean)[0], 'dl');

    const userIds: Array<string | undefined> = [];
    const response = new TestResponse();
    await streamDownload(
      { params: { token: download.pathname.split('/').at(-1)! } } as unknown as Request,
      response as unknown as Response,
      async ({ userId }) => {
        userIds.push(userId);
        return drive as never;
      },
    );

    assert.deepEqual(Buffer.concat(response.chunks), data);
    assert.equal(response.headers['Content-Length'], String(data.length));
    assert.deepEqual(userIds, ['google-sub-1']);

    const expiredResponse = new TestResponse();
    await streamDownload(
      { params: { token: download.pathname.split('/').at(-1)! } } as unknown as Request,
      expiredResponse as unknown as Response,
      async () => drive as never,
    );
    assert.equal(expiredResponse.statusCode, 404);
  } finally {
    if (oldBaseUrl === undefined) delete process.env.PUBLIC_BASE_URL;
    else process.env.PUBLIC_BASE_URL = oldBaseUrl;
  }
});

import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { streamBatchUpload } from '../../src/tools/largeFileTransfer.js';

test('single upload token survives transient Drive failure for retry and blocks concurrent reuse with 409', async () => {
  const oldBaseUrl = process.env.PUBLIC_BASE_URL;
  process.env.PUBLIC_BASE_URL = 'https://drive.example:8443';
  try {
    const data = Buffer.from('retryable-payload');
    const context = {
      userId: 'google-sub-1',
      uploadEndpointAvailable: true,
      accountAlias: 'work',
    } as unknown as ToolContext;
    const authClient = { getAccessToken: async () => ({ token: 'access-token' }) };

    const created = await handleTool('createUploadUrl', { name: 'retry.bin', sizeBytes: data.length }, context);
    assert.ok(created);
    const token = new URL(JSON.parse(created.content[0].text ?? '{}').uploadUrl).pathname.split('/').at(-1)!;

    // 1. Transient 503 failure -> token resets to pending
    const failFetch = (async (input: RequestInfo | URL) => {
      const url = new URL(String(input));
      if (url.hostname === 'www.googleapis.com') {
        return new Response(null, { status: 200, headers: { location: 'https://upload.example/session' } });
      }
      return new Response('temporary outage', { status: 503 });
    }) as typeof fetch;

    const firstResp = new TestResponse();
    await streamUpload(
      Object.assign(Readable.from([data]), {
        params: { token },
        headers: { 'content-length': String(data.length) },
      }) as unknown as Request,
      firstResp as unknown as Response,
      async () => authClient,
      failFetch,
    );
    assert.equal(firstResp.statusCode, 502);

    // 2. Retry with the SAME token succeeds, while a concurrent request during upload gets 409
    let releaseUpload!: () => void;
    const uploadStarted = new Promise<void>((resolve) => {
      releaseUpload = resolve;
    });
    let unblockUpload!: () => void;
    const uploadGate = new Promise<void>((resolve) => {
      unblockUpload = resolve;
    });

    const slowSuccessFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.hostname === 'www.googleapis.com') {
        return new Response(null, { status: 200, headers: { location: 'https://upload.example/session' } });
      }
      releaseUpload();
      await uploadGate;
      await new Response(init?.body as ReadableStream).arrayBuffer();
      return new Response(JSON.stringify({ id: 'retry-ok', name: 'retry.bin' }), { status: 200 });
    }) as typeof fetch;

    const retryResp = new TestResponse();
    const retryPromise = streamUpload(
      Object.assign(Readable.from([data]), {
        params: { token },
        headers: { 'content-length': String(data.length) },
      }) as unknown as Request,
      retryResp as unknown as Response,
      async () => authClient,
      slowSuccessFetch,
    );

    await uploadStarted;
    const concurrentResp = new TestResponse();
    await streamUpload(
      Object.assign(Readable.from([data]), {
        params: { token },
        headers: { 'content-length': String(data.length) },
      }) as unknown as Request,
      concurrentResp as unknown as Response,
      async () => authClient,
      slowSuccessFetch,
    );
    assert.equal(concurrentResp.statusCode, 409);

    unblockUpload();
    await retryPromise;
    assert.equal(retryResp.statusCode, 200);
    assert.equal(JSON.parse(Buffer.concat(retryResp.chunks).toString()).id, 'retry-ok');
  } finally {
    if (oldBaseUrl === undefined) delete process.env.PUBLIC_BASE_URL;
    else process.env.PUBLIC_BASE_URL = oldBaseUrl;
  }
});

test('createBatchUploadUrl + streamBatchUpload streams PAX TAR entries to Drive (create + in-place update + ignored + 207 missing)', async () => {
  const oldBaseUrl = process.env.PUBLIC_BASE_URL;
  process.env.PUBLIC_BASE_URL = 'https://drive.example:8443';
  const dir = mkdtempSync(join(tmpdir(), 'batch-upload-test-'));
  try {
    const epubData = Buffer.from('PK-epub-payload-'.repeat(200));
    const jsonData = Buffer.from('{"glossary":true}');
    const extraData = Buffer.from('ignored-extra');
    writeFileSync(join(dir, 'book.epub'), epubData);
    writeFileSync(join(dir, 'glossary.json'), jsonData);
    writeFileSync(join(dir, 'extra.txt'), extraData);
    const tarBuf = execFileSync('tar', ['--format=pax', '-cf', '-', '-C', dir, 'book.epub', 'glossary.json', 'extra.txt']);

    const uploaded = new Map<string, { method: string; body: Buffer; meta: Record<string, unknown> }>();
    let pendingTarget = '';
    let pendingMeta: Record<string, unknown> = {};
    let pendingMethod = '';
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.hostname === 'www.googleapis.com') {
        pendingMethod = init?.method ?? '';
        pendingMeta = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>;
        pendingTarget = String(pendingMeta.name ?? '');
        return new Response(null, { status: 200, headers: { location: `https://upload.example/${encodeURIComponent(pendingTarget)}` } });
      }
      const body = Buffer.from(await new Response(init?.body as ReadableStream).arrayBuffer());
      uploaded.set(pendingTarget, { method: pendingMethod, body, meta: pendingMeta });
      return new Response(JSON.stringify({ id: `id-${pendingTarget}`, name: pendingTarget, size: String(body.length) }), { status: 200 });
    }) as typeof fetch;

    const authClient = { getAccessToken: async () => ({ token: 'batch-token' }) };
    const context = { userId: 'u1', uploadEndpointAvailable: true } as unknown as ToolContext;

    const toolRes = await handleTool('createBatchUploadUrl', {
      files: [
        { archivePath: 'book.epub', name: 'Всеозброєний.epub', fileId: 'existing-epub-id', mimeType: 'application/epub+zip' },
        { archivePath: 'glossary.json', name: 'glossary.json', parentFolderId: 'folder-g', mimeType: 'application/json' },
      ],
    }, context);
    assert.ok(toolRes);
    const created = JSON.parse(toolRes.content[0].text ?? '{}');
    assert.equal(created.mode, 'batch_tar');
    assert.equal(created.fileCount, 2);
    const token = new URL(created.uploadUrl).pathname.split('/').at(-1)!;

    // Invalid TAR returns 400 and preserves token for retry
    const badResp = new TestResponse();
    await streamBatchUpload(
      Object.assign(Readable.from([Buffer.alloc(1024, 0x58)]), { params: { token }, headers: {} }) as unknown as Request,
      badResp as unknown as Response,
      async () => authClient,
      fetchImpl,
    );
    assert.equal(badResp.statusCode, 400);

    // Valid PAX TAR (chunked, no Content-Length!) succeeds with 200
    const resp = new TestResponse();
    await streamBatchUpload(
      Object.assign(Readable.from([tarBuf.subarray(0, 500), tarBuf.subarray(500)]), { params: { token }, headers: {} }) as unknown as Request,
      resp as unknown as Response,
      async () => authClient,
      fetchImpl,
    );
    assert.equal(resp.statusCode, 200);
    const summary = resp.body as { uploaded: Array<{ id: string; mode: string }>; failed: unknown[]; missing: string[]; ignored: string[] };
    assert.equal(summary.uploaded.length, 2);
    assert.equal(summary.failed.length, 0);
    assert.deepEqual(summary.missing, []);
    assert.deepEqual(summary.ignored, ['extra.txt']);
    assert.deepEqual(uploaded.get('Всеозброєний.epub')?.body, epubData);
    assert.equal(uploaded.get('Всеозброєний.epub')?.method, 'PATCH');
    assert.deepEqual(uploaded.get('glossary.json')?.body, jsonData);
    assert.equal(uploaded.get('glossary.json')?.method, 'POST');
  } finally {
    rmSync(dir, { recursive: true, force: true });
    if (oldBaseUrl === undefined) delete process.env.PUBLIC_BASE_URL;
    else process.env.PUBLIC_BASE_URL = oldBaseUrl;
  }
});

test('commitStagedUpload concatenates multi-chunk Drive staging files for raw in-place update and PAX TAR batch, with SHA-256 and cleanup', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'staged-commit-test-'));
  try {
    // --- Case 1: raw multi-chunk in-place update ---
    const fullRaw = Buffer.from('chunked-epub-content-'.repeat(150));
    const rawSha = createHash('sha256').update(fullRaw).digest('hex');
    const chunks = new Map<string, Buffer>([
      ['chunk-0', fullRaw.subarray(0, 1000)],
      ['chunk-1', fullRaw.subarray(1000, 2200)],
      ['chunk-2', fullRaw.subarray(2200)],
    ]);
    const deletedIds: string[] = [];
    const driveMock = {
      files: {
        get: async (params: { fileId: string; alt?: string }) => {
          const buf = chunks.get(params.fileId);
          if (!buf) throw new Error(`Missing ${params.fileId}`);
          if (params.alt === 'media') return { data: Readable.from([buf]) };
          return { data: { id: params.fileId, name: params.fileId, size: String(buf.length), trashed: false } };
        },
        delete: async (params: { fileId: string }) => {
          deletedIds.push(params.fileId);
          return {};
        },
      },
    };

    const uploadedRaw: Buffer[] = [];
    const methods: string[] = [];
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.hostname === 'www.googleapis.com') {
        methods.push(init?.method ?? '');
        return new Response(null, { status: 200, headers: { location: 'https://upload.example/raw-session' } });
      }
      const buf = Buffer.from(await new Response(init?.body as ReadableStream).arrayBuffer());
      uploadedRaw.push(buf);
      return new Response(JSON.stringify({ id: 'target-file-1', name: 'Overgeared_UA.epub', size: String(buf.length) }), { status: 200 });
    }) as typeof fetch;

    const ctx = {
      authClient: { getAccessToken: async () => ({ token: 'staged-token' }) },
      getDrive: () => driveMock,
    } as unknown as ToolContext;

    // Pre-flight size mismatch aborts BEFORE touching target or deleting chunks
    const badSizeRes = await handleTool('commitStagedUpload', {
      stagingFileIds: ['chunk-0', 'chunk-1', 'chunk-2'],
      name: 'Overgeared_UA.epub',
      fileId: 'target-file-1',
      expectedSizeBytes: fullRaw.length + 1,
    }, ctx, fetchImpl);
    assert.equal(badSizeRes?.isError, true);
    assert.equal(uploadedRaw.length, 0);
    assert.equal(deletedIds.length, 0);

    // Valid raw multi-chunk in-place commit
    const okRawRes = await handleTool('commitStagedUpload', {
      stagingFileIds: ['chunk-0', 'chunk-1', 'chunk-2'],
      name: 'Overgeared_UA.epub',
      mimeType: 'application/epub+zip',
      fileId: 'target-file-1',
      expectedSizeBytes: fullRaw.length,
      expectedSha256: rawSha,
    }, ctx, fetchImpl);
    assert.equal(okRawRes?.isError, undefined);
    const rawPayload = JSON.parse(okRawRes!.content[0].text ?? '{}');
    assert.equal(rawPayload.mode, 'update_in_place');
    assert.equal(rawPayload.sha256, rawSha);
    assert.deepEqual(rawPayload.stagingCleanup.deleted, ['chunk-0', 'chunk-1', 'chunk-2']);
    assert.deepEqual(uploadedRaw[0], fullRaw);
    assert.equal(methods[0], 'PATCH');

    // --- Case 2: PAX TAR split across 3 staging chunks ---
    const f1 = Buffer.from('file-one-bytes-'.repeat(80));
    const f2 = Buffer.from('file-two-bytes-'.repeat(60));
    writeFileSync(join(dir, 'one.epub'), f1);
    writeFileSync(join(dir, 'two.json'), f2);
    const tarBuf = execFileSync('tar', ['--format=pax', '-cf', '-', '-C', dir, 'one.epub', 'two.json']);
    const tarSha = createHash('sha256').update(tarBuf).digest('hex');

    chunks.clear();
    deletedIds.length = 0;
    const cut1 = Math.floor(tarBuf.length / 3);
    const cut2 = Math.floor((tarBuf.length * 2) / 3);
    chunks.set('tar-0', tarBuf.subarray(0, cut1));
    chunks.set('tar-1', tarBuf.subarray(cut1, cut2));
    chunks.set('tar-2', tarBuf.subarray(cut2));

    const tarUploaded = new Map<string, Buffer>();
    let activeName = '';
    const tarFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input));
      if (url.hostname === 'www.googleapis.com') {
        activeName = String((JSON.parse(String(init?.body ?? '{}')) as { name?: string }).name ?? '');
        return new Response(null, { status: 200, headers: { location: `https://upload.example/${activeName}` } });
      }
      const buf = Buffer.from(await new Response(init?.body as ReadableStream).arrayBuffer());
      tarUploaded.set(activeName, buf);
      return new Response(JSON.stringify({ id: `id-${activeName}`, name: activeName }), { status: 200 });
    }) as typeof fetch;

    const tarRes = await handleTool('commitStagedUpload', {
      stagingFileIds: ['tar-0', 'tar-1', 'tar-2'],
      format: 'tar',
      expectedSizeBytes: tarBuf.length,
      expectedSha256: tarSha,
      files: [
        { archivePath: 'one.epub', name: 'one.epub', fileId: 'exist-1', mimeType: 'application/epub+zip' },
        { archivePath: 'two.json', name: 'two.json', parentFolderId: 'folder-2', mimeType: 'application/json' },
      ],
    }, ctx, tarFetch);
    assert.equal(tarRes?.isError, undefined);
    const tarPayload = JSON.parse(tarRes!.content[0].text ?? '{}');
    assert.equal(tarPayload.format, 'tar');
    assert.equal(tarPayload.sha256, tarSha);
    assert.equal(tarPayload.uploaded.length, 2);
    assert.deepEqual(tarPayload.stagingCleanup.deleted, ['tar-0', 'tar-1', 'tar-2']);
    assert.deepEqual(tarUploaded.get('one.epub'), f1);
    assert.deepEqual(tarUploaded.get('two.json'), f2);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
