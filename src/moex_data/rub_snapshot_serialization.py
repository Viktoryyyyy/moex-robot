"""Lossless, self-contained representations of already validated RUB JSON.

Storage compresses canonical bytes. Delivery shares exact repeated subtrees using
JSON pointers, keeping every distinct value readable. Neither format is admission.
Expand before applying any original schema, evidence or freshness checks.
"""
from copy import deepcopy
from hashlib import sha256
import base64
import json
import zlib

STORAGE_SCHEMA = 'rub_snapshot_storage.v1'
DELIVERY_SCHEMA = 'rub_snapshot_references.v1'
SNAPSHOT_SCHEMA = 'rub_chat_analysis_snapshot.v1'
PACKAGE_SCHEMA = 'rub_factual_package.v1'
MIN_BYTES = 262144
MAX_BYTES = 128 * 1024 * 1024
REF = '$snapshot_ref'
LITERAL = '$snapshot_literal'


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def _envelope(value, raw, schema):
    return {'project': 'MOEX_Bot', 'schema_version': schema,
            'logical_schema_version': value['schema_version'],
            'expanded_bytes': len(raw), 'expanded_sha256': sha256(raw).hexdigest()}


def storage(value):
    """Only the primary snapshot; unrelated atomic-write callers stay unchanged."""
    if not isinstance(value, dict) or value.get('schema_version') != SNAPSHOT_SCHEMA:
        return value
    raw = encoded(value)
    if not MIN_BYTES <= len(raw) <= MAX_BYTES:
        return value
    result = {**_envelope(value, raw, STORAGE_SCHEMA), 'encoding': 'zlib+base64',
              'payload': base64.b64encode(zlib.compress(raw, level=6)).decode('ascii')}
    return result if len(encoded(result)) < len(raw) * .9 else value


def _token(key):
    return str(key).replace('~', '~0').replace('/', '~1')


def delivery(value):
    """Readable shared values, not compressed text or an abbreviated history."""
    if not isinstance(value, dict) or value.get('schema_version') not in (SNAPSHOT_SCHEMA, PACKAGE_SCHEMA):
        return value
    raw = encoded(value)
    if not MIN_BYTES <= len(raw) <= MAX_BYTES:
        return value
    seen = {}

    def visit(item, path):
        # Literal marker-shaped application data must never become a reference.
        if isinstance(item, dict) and set(item) in ({REF}, {LITERAL}):
            return {LITERAL: deepcopy(item)}
        data = encoded(item)
        if path and len(data) >= 512:
            digest = sha256(data).digest()
            prior = seen.get(digest)
            if prior is not None:
                return {REF: prior}
            seen[digest] = path
        if isinstance(item, dict):
            return {key: visit(child, path + '/' + _token(key)) for key, child in sorted(item.items())}
        if isinstance(item, list):
            return [visit(child, path + '/' + str(index)) for index, child in enumerate(item)]
        return item

    result = {**_envelope(value, raw, DELIVERY_SCHEMA),
        'reading_contract': 'data is the complete logical document. A sole $snapshot_ref replaces an exact duplicate; resolve its RFC 6901 pointer within data. A sole $snapshot_literal holds unchanged literal application data. Expand before schema/evidence/freshness checks. No history is omitted.',
        'data': visit(value, '')}
    return result if len(encoded(result)) < len(raw) * .9 else value


def _check_envelope(value):
    count = value.get('expanded_bytes')
    if value.get('project') != 'MOEX_Bot' or type(count) is not int or not 0 < count <= MAX_BYTES:
        raise ValueError('invalid snapshot representation identity/size')
    if value.get('logical_schema_version') not in (SNAPSHOT_SCHEMA, PACKAGE_SCHEMA):
        raise ValueError('unsupported logical snapshot schema')
    return count


def _inflate(value, count):
    if value.get('encoding') != 'zlib+base64' or not isinstance(value.get('payload'), str):
        raise ValueError('unsupported snapshot storage encoding')
    if len(value['payload']) > MAX_BYTES * 2:
        raise ValueError('snapshot storage payload too large')
    try:
        compressed = base64.b64decode(value['payload'], validate=True)
        decoder = zlib.decompressobj()
        raw = decoder.decompress(compressed, count + 1)
        if len(raw) != count or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            raise ValueError('snapshot storage length/stream mismatch')
        return raw
    except (zlib.error, TypeError) as exc:
        raise ValueError('invalid snapshot storage payload') from exc


def _expand_refs(data, limit):
    active = set()

    def locate(pointer):
        if not isinstance(pointer, str) or not pointer.startswith('/'):
            raise ValueError('invalid snapshot reference pointer')
        item = data
        for part in pointer[1:].split('/'):
            # Reject non-canonical escapes/array indices, not just missing paths.
            token = part.replace('~1', '/').replace('~0', '~')
            if _token(token) != part:
                raise ValueError('invalid snapshot reference escape')
            if isinstance(item, list):
                if not token.isascii() or not token.isdigit() or str(int(token)) != token:
                    raise ValueError('invalid snapshot array reference')
                index = int(token)
                if index >= len(item):
                    raise ValueError('missing snapshot array reference')
                item = item[index]
            elif isinstance(item, dict) and token in item:
                item = item[token]
            else:
                raise ValueError('missing snapshot reference')
        return item

    def visit(item, path, depth=0):
        if depth > 128 or path in active:
            raise ValueError('cyclic/deep snapshot reference')
        active.add(path)
        try:
            if isinstance(item, dict) and set(item) == {REF}:
                target = item[REF]
                result, size = visit(locate(target), target, depth + 1)
            elif isinstance(item, dict) and set(item) == {LITERAL}:
                result = deepcopy(item[LITERAL]); size = len(encoded(result))
            elif isinstance(item, (dict, list)):
                result = {} if isinstance(item, dict) else []
                size = 2
                children = item.items() if isinstance(item, dict) else enumerate(item)
                for index, (key, child) in enumerate(children):
                    restored, child_size = visit(child, path + '/' + _token(key), depth + 1)
                    size += child_size + (1 if index else 0)
                    if isinstance(item, dict):
                        size += len(encoded(key)) + 1
                    if size > limit:
                        raise ValueError('snapshot reference expansion exceeds declared size')
                    if isinstance(result, dict): result[key] = restored
                    else: result.append(restored)
            else:
                result = item; size = len(encoded(item))
            if size > limit:
                raise ValueError('snapshot reference expansion exceeds declared size')
            return result, size
        finally:
            active.remove(path)

    return visit(data, '')[0]


def expand(value):
    """Accept legacy expanded JSON; reject corrupt/unknown encoded documents."""
    if not isinstance(value, dict):
        return value
    schema = value.get('schema_version')
    if schema not in (STORAGE_SCHEMA, DELIVERY_SCHEMA):
        if isinstance(schema, str) and schema.startswith(('rub_snapshot_storage.', 'rub_snapshot_references.')):
            raise ValueError('unsupported snapshot representation version')
        return value
    count = _check_envelope(value)
    if schema == STORAGE_SCHEMA:
        if value['logical_schema_version'] != SNAPSHOT_SCHEMA:
            raise ValueError('storage requires primary snapshot schema')
        raw = _inflate(value, count)
        if sha256(raw).hexdigest() != value.get('expanded_sha256'):
            raise ValueError('snapshot storage digest mismatch')
        result = json.loads(raw)
        if encoded(result) != raw:
            raise ValueError('snapshot storage must contain canonical JSON')
    else:
        result = _expand_refs(value.get('data'), count)
        raw = encoded(result)
    if len(raw) != count or sha256(raw).hexdigest() != value.get('expanded_sha256'):
        raise ValueError('snapshot representation digest/length mismatch')
    if not isinstance(result, dict) or result.get('schema_version') != value['logical_schema_version']:
        raise ValueError('snapshot representation logical identity mismatch')
    return result


def loads(raw):
    return expand(json.loads(raw))


def main(argv=None):
    """Decode an existing carrier to stdout without refreshing or changing clocks."""
    import argparse
    from pathlib import Path
    import sys
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expand', type=Path, required=True, metavar='FILE')
    args = parser.parse_args(argv)
    raw = encoded(loads(args.expand.read_bytes()))
    sys.stdout.buffer.write(raw + b'\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
