type MsgpackValue = null | boolean | number | string | Uint8Array | MsgpackValue[] | { [key: string]: MsgpackValue };

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function encodeVoiceIndication(payload: Record<string, unknown>): Uint8Array {
  return encodeMessage({ Indication: { data: payload as MsgpackValue } });
}

export function encodeMessage(value: MsgpackValue): Uint8Array {
  const chunks: Uint8Array[] = [];
  write(value);
  const result = new Uint8Array(chunks.reduce((size, chunk) => size + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) { result.set(chunk, offset); offset += chunk.length; }
  return result;

  function pushHeader(header: number, length: number, width: 1 | 2 | 4) {
    const chunk = new Uint8Array(1 + width);
    chunk[0] = header;
    const view = new DataView(chunk.buffer);
    if (width === 1) view.setUint8(1, length);
    else if (width === 2) view.setUint16(1, length, false);
    else view.setUint32(1, length, false);
    chunks.push(chunk);
  }

  function write(value: MsgpackValue | undefined): void {
    if (value == null) { chunks.push(new Uint8Array([0xc0])); return; }
    if (typeof value === 'boolean') { chunks.push(new Uint8Array([value ? 0xc3 : 0xc2])); return; }
    if (typeof value === 'number') { writeNumber(value); return; }
    if (typeof value === 'string') {
      const bytes = textEncoder.encode(value);
      if (bytes.length < 32) chunks.push(new Uint8Array([0xa0 + bytes.length]));
      else if (bytes.length < 256) pushHeader(0xd9, bytes.length, 1);
      else if (bytes.length <= 0xffff) pushHeader(0xda, bytes.length, 2);
      else pushHeader(0xdb, bytes.length, 4);
      chunks.push(bytes);
      return;
    }
    if (value instanceof Uint8Array) {
      if (value.length < 256) pushHeader(0xc4, value.length, 1);
      else if (value.length <= 0xffff) pushHeader(0xc5, value.length, 2);
      else pushHeader(0xc6, value.length, 4);
      chunks.push(value);
      return;
    }
    if (Array.isArray(value)) {
      if (value.length < 16) chunks.push(new Uint8Array([0x90 + value.length]));
      else if (value.length <= 0xffff) pushHeader(0xdc, value.length, 2);
      else pushHeader(0xdd, value.length, 4);
      value.forEach(write);
      return;
    }
    const keys = Object.keys(value);
    if (keys.length < 16) chunks.push(new Uint8Array([0x80 + keys.length]));
    else if (keys.length <= 0xffff) pushHeader(0xde, keys.length, 2);
    else pushHeader(0xdf, keys.length, 4);
    for (const key of keys) { write(key); write(value[key]); }
  }

  function writeNumber(value: number) {
    if (!Number.isInteger(value)) {
      const chunk = new Uint8Array(9); chunk[0] = 0xcb;
      new DataView(chunk.buffer).setFloat64(1, value, false); chunks.push(chunk); return;
    }
    if (value >= 0) {
      if (value <= 0x7f) chunks.push(new Uint8Array([value]));
      else if (value <= 0xff) { const c = new Uint8Array([0xcc, value]); chunks.push(c); }
      else if (value <= 0xffff) { const c = new Uint8Array(3); c[0] = 0xcd; new DataView(c.buffer).setUint16(1, value, false); chunks.push(c); }
      else if (value <= 0xffffffff) { const c = new Uint8Array(5); c[0] = 0xce; new DataView(c.buffer).setUint32(1, value, false); chunks.push(c); }
      else { const c = new Uint8Array(9); c[0] = 0xcf; new DataView(c.buffer).setBigUint64(1, BigInt(value), false); chunks.push(c); }
    } else if (value >= -32) chunks.push(new Uint8Array([0x100 + value]));
    else if (value >= -128) { const c = new Uint8Array([0xd0, value + 256]); chunks.push(c); }
    else if (value >= -32768) { const c = new Uint8Array(3); c[0] = 0xd1; new DataView(c.buffer).setInt16(1, value, false); chunks.push(c); }
    else if (value >= -2147483648) { const c = new Uint8Array(5); c[0] = 0xd2; new DataView(c.buffer).setInt32(1, value, false); chunks.push(c); }
    else { const c = new Uint8Array(9); c[0] = 0xd3; new DataView(c.buffer).setBigInt64(1, BigInt(value), false); chunks.push(c); }
  }
}

export function decodeVoiceMessage(input: ArrayBuffer | Uint8Array): Record<string, any> {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  const root = new Reader(bytes).read();
  if (!root || typeof root !== 'object' || Array.isArray(root)) throw new Error('MessagePack envelope must be a map');
  const envelope = (root as Record<string, any>).Indication ?? (root as Record<string, any>).ClientCommand ?? (root as Record<string, any>).ServerCommand;
  const payload = envelope?.data ?? envelope?.command;
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw new Error('MessagePack envelope has no payload');
  return payload as Record<string, any>;
}

class Reader {
  private offset = 0;
  constructor(private readonly data: Uint8Array) {}
  read(): any {
    const tag = this.byte();
    if (tag <= 0x7f) return tag;
    if (tag >= 0xe0) return tag - 0x100;
    if (tag >= 0xa0 && tag <= 0xbf) return this.string(tag - 0xa0);
    if (tag >= 0x90 && tag <= 0x9f) return this.array(tag - 0x90);
    if (tag >= 0x80 && tag <= 0x8f) return this.map(tag - 0x80);
    if (tag >= 0xc0 && tag <= 0xc3) return tag === 0xc0 ? null : tag === 0xc3;
    if (tag === 0xcc) return this.byte();
    if (tag === 0xcd) return this.uint(2);
    if (tag === 0xce) return this.uint(4);
    if (tag === 0xcf) return Number(this.bigUint(8));
    if (tag === 0xd0) return this.int(1);
    if (tag === 0xd1) return this.int(2);
    if (tag === 0xd2) return this.int(4);
    if (tag === 0xd3) return Number(this.bigInt(8));
    if (tag === 0xca) return this.view().getFloat32(this.advance(4), false);
    if (tag === 0xcb) return this.view().getFloat64(this.advance(8), false);
    if (tag === 0xd9) return this.string(this.byte());
    if (tag === 0xda) return this.string(this.uint(2));
    if (tag === 0xdb) return this.string(this.uint(4));
    if (tag === 0xc4) return this.bytes(this.byte());
    if (tag === 0xc5) return this.bytes(this.uint(2));
    if (tag === 0xc6) return this.bytes(this.uint(4));
    if (tag === 0xdc) return this.array(this.uint(2));
    if (tag === 0xdd) return this.array(this.uint(4));
    if (tag === 0xde) return this.map(this.uint(2));
    if (tag === 0xdf) return this.map(this.uint(4));
    throw new Error(`MessagePack tag 0x${tag.toString(16)} is unsupported`);
  }
  private byte() { if (this.offset >= this.data.length) throw new Error('Unexpected end of MessagePack data'); return this.data[this.offset++]; }
  private advance(length: number) { const offset = this.offset; this.offset += length; if (this.offset > this.data.length) throw new Error('Unexpected end of MessagePack data'); return offset; }
  private view() { return new DataView(this.data.buffer, this.data.byteOffset); }
  private uint(length: number) { return length === 2 ? this.view().getUint16(this.advance(length), false) : this.view().getUint32(this.advance(length), false); }
  private int(length: number) { const offset = this.advance(length); return length === 1 ? this.view().getInt8(offset) : length === 2 ? this.view().getInt16(offset, false) : this.view().getInt32(offset, false); }
  private bigUint(length: number) { return this.view().getBigUint64(this.advance(length), false); }
  private bigInt(length: number) { return this.view().getBigInt64(this.advance(length), false); }
  private string(length: number) { return textDecoder.decode(this.data.slice(this.advance(length), this.offset)); }
  private bytes(length: number) { return this.data.slice(this.advance(length), this.offset); }
  private array(length: number) { return Array.from({ length }, () => this.read()); }
  private map(length: number) { const result: Record<string, any> = {}; for (let i = 0; i < length; i += 1) result[String(this.read())] = this.read(); return result; }
}
