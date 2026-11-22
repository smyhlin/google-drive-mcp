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
