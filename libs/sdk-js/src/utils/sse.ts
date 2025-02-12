const mergeArrays = (a: ArrayLike<number>, b: ArrayLike<number>) => {
  const mergedArray = new Uint8Array(a.length + b.length);
  mergedArray.set(a);
  mergedArray.set(b, a.length);
  return mergedArray;
};

const CR = "\r".charCodeAt(0);
const LF = "\n".charCodeAt(0);
const NULL = "\0".charCodeAt(0);
const COLON = ":".charCodeAt(0);
const SPACE = " ".charCodeAt(0);

const TRAILING_NEWLINE = [CR, LF];

export class BytesLineDecoder extends TransformStream<Uint8Array, Uint8Array> {
  constructor() {
    let buffer: Uint8Array[] = [];
    let trailingCr = false;

    super({
      start() {
        buffer = [];
        trailingCr = false;
      },

      transform(chunk, controller) {
        // See https://docs.python.org/3/glossary.html#term-universal-newlines
        let text = chunk;

        // Handle trailing CR from previous chunk
        if (trailingCr) {
          text = mergeArrays([CR], text);
          trailingCr = false;
        }

        // Check for trailing CR in current chunk
        if (text.length > 0 && text.at(-1) === CR) {
          trailingCr = true;
          text = text.subarray(0, -1);
        }

        if (!text.length) return;
        const trailingNewline = TRAILING_NEWLINE.includes(text.at(-1)!);

        const lastIdx = text.length - 1;
        const [lines] = text.reduce(
          (acc, cur, idx) => {
            if (cur === CR) {
              if (text[idx + 1] === LF) {
                acc[0].push(text.subarray(acc[1], idx));
                acc[1] = idx + 2;
              } else {
                acc[0].push(text.subarray(acc[1], idx));
                acc[1] = idx + 1;
              }
            } else if (cur === LF && text[idx - 1] !== CR) {
              acc[0].push(text.subarray(acc[1], idx));
              acc[1] = idx + 1;
            }
            if (idx === lastIdx && acc[1] < lastIdx) {
              acc[0].push(text.subarray(acc[1]));
            }
            return acc;
          },
          [[], 0] as [Uint8Array[], number],
        );

        if (lines.length === 1 && !trailingNewline) {
          buffer.push(lines[0]);
          return;
        }

        if (buffer.length) {
          // Include existing buffer in first line
          buffer.push(lines[0]);

          lines[0] = joinArrays(buffer);

          buffer = [];
        }

        if (!trailingNewline) {
          // If the last segment is not newline terminated,
          // buffer it for the next chunk
          if (lines.length) buffer = [lines.pop()!];
        }

        // Enqueue complete lines
        for (const line of lines) {
          controller.enqueue(line);
        }
      },

      flush(controller) {
        if (buffer.length) {
          controller.enqueue(joinArrays(buffer));
        }
      },
    });
  }
}

interface StreamPart {
  event: string;
  data: unknown;
}

export class SSEDecoder extends TransformStream<Uint8Array, StreamPart> {
  constructor() {
    let event = "";
    let data: Uint8Array[] = [];
    let lastEventId = "";
    let retry: number | null = null;

    const decoder = new TextDecoder();

    super({
      transform(chunk, controller) {
        // Handle empty line case
        if (!chunk.length) {
          if (!event && !data.length && !lastEventId && retry == null) return;

          const sse = {
            event,
            data: data.length ? decodeArraysToJson(decoder, data) : null,
          };

          // NOTE: as per the SSE spec, do not reset lastEventId
          event = "";
          data = [];
          retry = null;

          controller.enqueue(sse);
          return;
        }

        // Ignore comments
        if (chunk[0] === COLON) return;

        const sepIdx = chunk.indexOf(COLON);
        if (sepIdx === -1) return;

        const fieldName = decoder.decode(chunk.subarray(0, sepIdx));
        let value = chunk.subarray(sepIdx + 1);
        if (value[0] === SPACE) value = value.subarray(1);

        if (fieldName === "event") {
          event = decoder.decode(value);
        } else if (fieldName === "data") {
          data.push(value);
        } else if (fieldName === "id") {
          if (value.indexOf(NULL) === -1) lastEventId = decoder.decode(value);
        } else if (fieldName === "retry") {
          const retryNum = Number.parseInt(decoder.decode(value));
          if (!Number.isNaN(retryNum)) retry = retryNum;
        }
      },

      flush(controller) {
        if (event) {
          controller.enqueue({
            event,
            data: data.length ? decodeArraysToJson(decoder, data) : null,
          });
        }
      },
    });
  }
}

function decodeArraysToJson(decoder: TextDecoder, arrays: Uint8Array[]) {
  const totalLength = arrays.reduce((acc, curr) => acc + curr.length, 0);
  let merged = new Uint8Array(totalLength);
  let offset = 0;
  for (const c of arrays) {
    merged.set(c, offset);
    offset += c.length;
  }
  return JSON.parse(decoder.decode(merged));
}

function joinArrays(arrays: Uint8Array[]) {
  const totalLength = arrays.reduce((acc, curr) => acc + curr.length, 0);
  let merged = new Uint8Array(totalLength);
  let offset = 0;
  for (const c of arrays) {
    merged.set(c, offset);
    offset += c.length;
  }
  return merged;
}
