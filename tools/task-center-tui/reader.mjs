import fs from 'node:fs';
import { StringDecoder } from 'node:string_decoder';
import { Model } from './model.mjs';

// Read-only transport. A bounded read per tick keeps keyboard input responsive.
export class JournalReader {
  model = new Model();
  offset = 0;
  tail = '';
  decoder = new StringDecoder('utf8');
  identity = null;
  error = '';
  exists = false;
  pending = false;
  lastRead = null;
  constructor(file) { this.file = file; }

  refresh(budget = 4 * 1024 * 1024) {
    if (this.error) return;
    let fd;
    try {
      fd = fs.openSync(this.file, 'r');
      const stat = fs.fstatSync(fd);
      const identity = `${stat.dev}:${stat.ino}`;
      if ((this.identity && identity !== this.identity) || stat.size < this.offset) {
        throw Error('Journal replaced/truncated. Reopen viewer; streams cannot be merged.');
      }
      this.exists = true;
      this.identity = identity;
      const buffer = Buffer.alloc(64 * 1024);
      while (budget > 0) {
        const n = fs.readSync(fd, buffer, 0, Math.min(buffer.length, budget), this.offset);
        if (!n) break;
        this.offset += n;
        budget -= n;
        this.tail += this.decoder.write(buffer.subarray(0, n));
        let end;
        while ((end = this.tail.indexOf('\n')) >= 0) {
          const line = this.tail.slice(0, end);
          if (Buffer.byteLength(line) > 16 * 1024 * 1024) throw Error('Audit frame exceeds 16 MiB');
          this.model.apply(JSON.parse(line));
          this.tail = this.tail.slice(end + 1);
          this.lastRead = Date.now();
        }
        if (Buffer.byteLength(this.tail) > 16 * 1024 * 1024) throw Error('Audit frame exceeds 16 MiB');
      }
      this.pending = this.offset < stat.size;
    } catch (error) {
      if (error.code === 'ENOENT' && !this.identity) this.exists = false;
      else this.error = error.message;
    } finally {
      if (fd !== undefined) fs.closeSync(fd);
    }
  }

  snapshot() {
    return { file: this.file, error: this.error, exists: this.exists, pending: this.pending,
      partial: !!this.tail, lastRead: this.lastRead };
  }
}
