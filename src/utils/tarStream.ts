import { PassThrough, Readable } from 'node:stream';

const BLOCK_SIZE = 512;
const MAX_EXTENDED_HEADER_BYTES = 1024 * 1024; // 1 MiB safety cap for PAX / GNU LongLink metadata

export class TarFormatError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'TarFormatError';
  }
}

export interface TarEntryHeader {
  /** Normalized relative POSIX path (no leading ./ or /, no .. segments). */
  path: string;
  /** Raw unnormalized path from header. */
  rawPath: string;
  /** Exact payload size in bytes. */
  size: number;
  /** Single-character TAR typeflag ('0' or '\0' for regular file). */
  typeflag: string;
  /** True when the entry is a regular file. */
  isFile: boolean;
}

export interface TarEntry {
  header: TarEntryHeader;
  /** Exact-length readable stream for this entry's payload. Must be consumed or drained before the callback returns. */
  stream: Readable;
}

/**
 * Normalize an archive path for safe allowlist matching.
 * Strips leading `/` and `./`, collapses repeated `/`, and rejects `..` traversal or NUL bytes.
 */
export function normalizeArchivePath(rawPath: string): string {
  if (rawPath.includes('\0')) {
    throw new TarFormatError('Archive path contains a NUL byte');
  }
  const slashNormalized = rawPath.replace(/\\/g, '/');
  const trimmed = slashNormalized.replace(/^\/+/, '');
  const parts = trimmed.split('/').filter((segment) => segment.length > 0 && segment !== '.');
  if (parts.length === 0) {
    throw new TarFormatError(`Archive path "${rawPath}" resolves to an empty path`);
  }
  for (const part of parts) {
    if (part === '..') {
      throw new TarFormatError(`Archive path "${rawPath}" contains forbidden ".." traversal`);
    }
  }
  return parts.join('/');
}

function isZeroBlock(block: Buffer): boolean {
  for (let i = 0; i < block.length; i += 1) {
    if (block[i] !== 0) return false;
  }
  return true;
}

function readNullTerminatedString(buf: Buffer, offset: number, length: number): string {
  let end = offset;
  const max = offset + length;
  while (end < max && buf[end] !== 0) end += 1;
  return buf.subarray(offset, end).toString('utf8');
}

function parseOctal(buf: Buffer, offset: number, length: number): number {
  const raw = readNullTerminatedString(buf, offset, length).trim();
  if (!raw) return 0;
  if (!/^[0-7]+$/.test(raw)) {
    throw new TarFormatError(`Invalid octal header field: "${raw}"`);
  }
  const value = Number.parseInt(raw, 8);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TarFormatError(`Octal header field out of safe integer range: "${raw}"`);
  }
  return value;
}

/**
 * Parses a TAR size field supporting both standard ASCII octal and GNU base-256 binary encoding.
 */
function parseSizeField(buf: Buffer, offset: number, length: number): number {
  const first = buf[offset];
  if ((first & 0x80) !== 0) {
    // GNU base-256 big-endian binary encoding
    if ((first & 0x40) !== 0) {
      throw new TarFormatError('Negative TAR entry size is not supported');
    }
    let value = BigInt(first & 0x7f);
    for (let i = 1; i < length; i += 1) {
      value = (value << 8n) | BigInt(buf[offset + i]);
    }
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new TarFormatError('TAR entry size exceeds MAX_SAFE_INTEGER');
    }
    return Number(value);
  }
  return parseOctal(buf, offset, length);
}

function verifyHeaderChecksum(block: Buffer): void {
  const expected = parseOctal(block, 148, 8);
  let unsignedSum = 0;
  let signedSum = 0;
  for (let i = 0; i < BLOCK_SIZE; i += 1) {
    const byte = i >= 148 && i < 156 ? 0x20 : block[i];
    unsignedSum += byte;
    signedSum += byte < 128 ? byte : byte - 256;
  }
  if (unsignedSum !== expected && signedSum !== expected) {
    throw new TarFormatError('Invalid TAR header checksum');
  }
}

/**
 * Parses POSIX.1-2001 PAX extended header records: `<length> <key>=<value>\n`
 * where `<length>` is the byte length of the entire record in UTF-8.
 */
function parsePaxRecords(data: Buffer): Map<string, string> {
  const records = new Map<string, string>();
  let offset = 0;
  while (offset < data.length) {
    const spaceIdx = data.indexOf(0x20, offset);
    if (spaceIdx === -1) break;
    const lenStr = data.subarray(offset, spaceIdx).toString('ascii');
    if (!/^[0-9]+$/.test(lenStr)) {
      throw new TarFormatError(`Invalid PAX record length: "${lenStr}"`);
    }
    const recordLen = Number.parseInt(lenStr, 10);
    if (!Number.isSafeInteger(recordLen) || recordLen <= spaceIdx - offset + 1 || offset + recordLen > data.length) {
      throw new TarFormatError(`Malformed PAX record length: ${recordLen}`);
    }
    const recordEnd = offset + recordLen;
    // Strip trailing newline
    const bodyEnd = data[recordEnd - 1] === 0x0a ? recordEnd - 1 : recordEnd;
    const body = data.subarray(spaceIdx + 1, bodyEnd).toString('utf8');
    const eqIdx = body.indexOf('=');
    if (eqIdx !== -1) {
      records.set(body.slice(0, eqIdx), body.slice(eqIdx + 1));
    }
    offset = recordEnd;
  }
  return records;
}

class ChunkReader {
  private readonly iterator: AsyncIterator<Buffer>;
  private buffer: Buffer = Buffer.alloc(0);
  private offset = 0;
  private eof = false;

  constructor(source: AsyncIterable<Buffer | Uint8Array> | Iterable<Buffer | Uint8Array>) {
    this.iterator = (async function* () {
      for await (const chunk of source) {
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        if (buf.length > 0) yield buf;
      }
    })();
  }

  private compact(): void {
    if (this.offset > 0) {
      this.buffer = this.buffer.subarray(this.offset);
      this.offset = 0;
    }
  }

  private async pullMore(): Promise<boolean> {
    if (this.eof) return false;
    const next = await this.iterator.next();
    if (next.done) {
      this.eof = true;
      return false;
    }
    this.compact();
    this.buffer = this.buffer.length === 0 ? next.value : Buffer.concat([this.buffer, next.value]);
    return true;
  }

  async readExact(bytes: number): Promise<Buffer | null> {
    if (bytes === 0) return Buffer.alloc(0);
    while (this.buffer.length - this.offset < bytes) {
      const gotMore = await this.pullMore();
      if (!gotMore) {
        if (this.buffer.length - this.offset === 0) return null;
        throw new TarFormatError('Unexpected end of TAR stream');
      }
    }
    const slice = this.buffer.subarray(this.offset, this.offset + bytes);
    this.offset += bytes;
    if (this.offset === this.buffer.length) {
      this.buffer = Buffer.alloc(0);
      this.offset = 0;
    }
    return slice;
  }

  async skipExact(bytes: number): Promise<void> {
    let remaining = bytes;
    while (remaining > 0) {
      const available = this.buffer.length - this.offset;
      if (available === 0) {
        const gotMore = await this.pullMore();
        if (!gotMore) {
          throw new TarFormatError('Unexpected end of TAR stream while skipping padding');
        }
        continue;
      }
      const take = Math.min(available, remaining);
      this.offset += take;
      remaining -= take;
      if (this.offset === this.buffer.length) {
        this.buffer = Buffer.alloc(0);
        this.offset = 0;
      }
    }
  }

  /**
   * Pipes exactly `bytes` from the upstream reader into `target` with backpressure.
   */
  async pipeExact(bytes: number, target: PassThrough): Promise<void> {
    let remaining = bytes;
    try {
      while (remaining > 0) {
        const available = this.buffer.length - this.offset;
        if (available === 0) {
          const gotMore = await this.pullMore();
          if (!gotMore) {
            throw new TarFormatError('Unexpected end of TAR stream inside file payload');
          }
          continue;
        }
        const take = Math.min(available, remaining);
        const chunk = this.buffer.subarray(this.offset, this.offset + take);
        this.offset += take;
        remaining -= take;
        if (this.offset === this.buffer.length) {
          this.buffer = Buffer.alloc(0);
          this.offset = 0;
        }
        if (target.destroyed) {
          // Consumer aborted early; still drain remaining bytes so stream stays aligned
          continue;
        }
        if (!target.write(chunk)) {
          await new Promise<void>((resolve, reject) => {
            const onDrain = () => {
              cleanup();
              resolve();
            };
            const onClose = () => {
              cleanup();
              resolve();
            };
            const onError = (err: Error) => {
              cleanup();
              reject(err);
            };
            const cleanup = () => {
              target.off('drain', onDrain);
              target.off('close', onClose);
              target.off('error', onError);
            };
            target.once('drain', onDrain);
            target.once('close', onClose);
            target.once('error', onError);
          });
        }
      }
      if (!target.destroyed) {
        target.end();
      }
    } catch (err) {
      if (!target.destroyed) {
        target.destroy(err instanceof Error ? err : new Error(String(err)));
      }
      throw err;
    }
  }
}

/**
 * Streams entries from a POSIX USTAR / PAX / GNU TAR archive in O(1) memory.
 * Each entry's `stream` receives exact `header.size` bytes with full backpressure.
 */
export async function parseTarStream(
  source: AsyncIterable<Buffer | Uint8Array> | Iterable<Buffer | Uint8Array>,
  onEntry: (entry: TarEntry) => Promise<void>,
): Promise<void> {
  const reader = new ChunkReader(source);
  let paxGlobal = new Map<string, string>();
  let paxNext: Map<string, string> | null = null;
  let gnuLongName: string | null = null;
  let zeroBlocks = 0;

  while (true) {
    const headerBlock = await reader.readExact(BLOCK_SIZE);
    if (headerBlock === null) break;

    if (isZeroBlock(headerBlock)) {
      zeroBlocks += 1;
      if (zeroBlocks >= 2) break;
      continue;
    }
    zeroBlocks = 0;

    verifyHeaderChecksum(headerBlock);

    const nameField = readNullTerminatedString(headerBlock, 0, 100);
    const prefixField = readNullTerminatedString(headerBlock, 345, 155);
    const rawType = headerBlock[156];
    const typeflag = rawType === 0 ? '\0' : String.fromCharCode(rawType);
    const headerSize = parseSizeField(headerBlock, 124, 12);
    const padBytes = (BLOCK_SIZE - (headerSize % BLOCK_SIZE)) % BLOCK_SIZE;

    // PAX extended header for next entry ('x') or global ('g')
    if (typeflag === 'x' || typeflag === 'g') {
      if (headerSize > MAX_EXTENDED_HEADER_BYTES) {
        throw new TarFormatError(`PAX header exceeds maximum allowed size (${MAX_EXTENDED_HEADER_BYTES} bytes)`);
      }
      const paxBuf = (await reader.readExact(headerSize)) ?? Buffer.alloc(0);
      await reader.skipExact(padBytes);
      const parsed = parsePaxRecords(paxBuf);
      if (typeflag === 'g') {
        paxGlobal = new Map([...paxGlobal, ...parsed]);
      } else {
        paxNext = parsed;
      }
      continue;
    }

    // GNU LongLink path ('L')
    if (typeflag === 'L') {
      if (headerSize > MAX_EXTENDED_HEADER_BYTES) {
        throw new TarFormatError(`GNU LongLink header exceeds maximum allowed size (${MAX_EXTENDED_HEADER_BYTES} bytes)`);
      }
      const longBuf = (await reader.readExact(headerSize)) ?? Buffer.alloc(0);
      await reader.skipExact(padBytes);
      gnuLongName = readNullTerminatedString(longBuf, 0, longBuf.length);
      continue;
    }

    // Resolve effective path & size (PAX next > GNU LongLink > PAX global > USTAR prefix/name)
    const effectivePaxPath = paxNext?.get('path') ?? paxGlobal.get('path');
    const effectivePaxSize = paxNext?.get('size') ?? paxGlobal.get('size');
    const ustarPath = prefixField ? `${prefixField}/${nameField}` : nameField;
    const rawPath = effectivePaxPath ?? gnuLongName ?? ustarPath;

    let size = headerSize;
    if (effectivePaxSize !== undefined) {
      if (!/^[0-9]+$/.test(effectivePaxSize)) {
        throw new TarFormatError(`Invalid PAX size value: "${effectivePaxSize}"`);
      }
      const parsedPaxSize = Number(effectivePaxSize);
      if (!Number.isSafeInteger(parsedPaxSize) || parsedPaxSize < 0) {
        throw new TarFormatError(`PAX size out of safe integer range: "${effectivePaxSize}"`);
      }
      size = parsedPaxSize;
    }

    paxNext = null;
    gnuLongName = null;

    const entryPadBytes = (BLOCK_SIZE - (size % BLOCK_SIZE)) % BLOCK_SIZE;
    const isFile = typeflag === '0' || typeflag === '\0';

    if (!isFile) {
      // Skip non-regular entries (directories, symlinks, etc.) cleanly
      await reader.skipExact(size + entryPadBytes);
      continue;
    }

    const normalizedPath = normalizeArchivePath(rawPath);
    const entryStream = new PassThrough();

    // Run producer (pipeExact) and consumer (onEntry) concurrently with backpressure
    let consumerError: unknown = null;
    const consumerPromise = onEntry({
      header: {
        path: normalizedPath,
        rawPath,
        size,
        typeflag,
        isFile: true,
      },
      stream: entryStream,
    })
      .catch((err) => {
        consumerError = err;
      })
      .finally(() => {
        entryStream.resume();
      });

    await reader.pipeExact(size, entryStream);
    await consumerPromise;
    if (consumerError !== null) {
      throw consumerError;
    }

    await reader.skipExact(entryPadBytes);
  }
}
