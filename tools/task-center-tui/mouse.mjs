// SGR terminal mouse packets. No global desktop clicks or business actions.
export class MouseInput {
  pending = '';
  feed(chunk) {
    this.pending += chunk.toString();
    const events = [];
    const pattern = new RegExp(String.fromCharCode(27) + '\\[<(\\d+);(\\d+);(\\d+)([mM])', 'g');
    let match, consumed = 0;
    while ((match = pattern.exec(this.pending))) {
      consumed = pattern.lastIndex;
      const button = Number(match[1]);
      if (match[4] !== 'M') continue;
      if (button === 0 || button === 64 || button === 65) events.push({
        x:Number(match[2])-1, y:Number(match[3])-1, wheel:button===64?-1:button===65?1:0,
      });
    }
    const remainder = this.pending.slice(consumed), start = remainder.lastIndexOf('\x1b');
    this.pending = start < 0 ? '' : remainder.slice(start).slice(0,64);
    return events;
  }
}
export function hitTest(frame, x, y) {
  return frame?.hits.findLast(hit => x>=hit.x && x<hit.x+hit.width && y>=hit.y && y<hit.y+hit.height);
}
