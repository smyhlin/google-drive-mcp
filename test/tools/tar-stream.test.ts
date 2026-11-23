import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { normalizeArchivePath, parseTarStream, TarFormatError } from '../../src/utils/tarStream.js';

function chunkBuffer(buf: Buffer, chunkSize: number): Buffer[] {
  const chunks: Buffer[] = [];
  for (let i = 0; i < buf.length; i += chunkSize) {
    chunks.push(buf.subarray(i, Math.min(i + chunkSize, buf.length)));
  }
  return chunks;
}

async function readStream(stream: AsyncIterable<Buffer | Uint8Array>): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

test('normalizeArchivePath strips leading ./ and / and rejects traversal', () => {
  assert.equal(normalizeArchivePath('./books/book.epub'), 'books/book.epub');
  assert.equal(normalizeArchivePath('/a//b/./c.json'), 'a/b/c.json');
  assert.throws(() => normalizeArchivePath('../secret.txt'), TarFormatError);
  assert.throws(() => normalizeArchivePath('a/../../b'), TarFormatError);
  assert.throws(() => normalizeArchivePath('bad\0name'), TarFormatError);
});

test('parseTarStream parses real PAX and GNU TAR archives with long Ukrainian paths under tiny chunking', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'tar-stream-test-'));
  try {
    const longUaDir = 'Переклади_ранобе_дуже_довга_назва_папки_для_перевірки_PAX_та_GNU_LongLink';
    const longUaFile = `${longUaDir}/Всеозброєний_Том_01_фінальна_вичитка_після_аудиту_ukling_v1.4.0.epub`;
    mkdirSync(join(dir, longUaDir), { recursive: true });
    const payload1 = Buffer.from('binary-epub-content-'.repeat(400));
    const payload2 = Buffer.from(JSON.stringify({ version: '2.1', origin_language: 'ko' }));
    const payload3 = Buffer.alloc(0);

    writeFileSync(join(dir, longUaFile), payload1);
    writeFileSync(join(dir, 'glossary.json'), payload2);
    writeFileSync(join(dir, 'empty.txt'), payload3);

    for (const format of ['pax', 'gnu'] as const) {
      const tarBuf = execFileSync('tar', [`--format=${format}`, '-cf', '-', '-C', dir, longUaFile, 'glossary.json', 'empty.txt']);
      const extracted = new Map<string, Buffer>();
      await parseTarStream(chunkBuffer(tarBuf, 137), async ({ header, stream }) => {
        extracted.set(header.path, await readStream(stream));
      });
      assert.deepEqual(extracted.get(longUaFile), payload1, `PAX/GNU (${format}) long Ukrainian file mismatch`);
      assert.deepEqual(extracted.get('glossary.json'), payload2);
      assert.deepEqual(extracted.get('empty.txt'), payload3);
      assert.equal(extracted.size, 3);
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('parseTarStream rejects corrupted headers and truncated streams with TarFormatError', async () => {
  const garbage = Buffer.alloc(1024, 0x41);
  await assert.rejects(() => parseTarStream([garbage], async () => {}), TarFormatError);

  const truncated = Buffer.alloc(100, 0x00);
  await assert.rejects(() => parseTarStream([truncated], async () => {}), TarFormatError);
});
