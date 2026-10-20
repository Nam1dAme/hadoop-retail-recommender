"""Frozen UTF-8, LF, no-header TSV codec (docs/04)."""
NULL = '\\N'


def encode(value):
    if value is None:
        return NULL
    text = str(value)
    if any(c in text for c in '\t\r\n\x00'):
        raise ValueError('TSV field contains a forbidden control character')
    return text.replace('\\', '\\\\')


def decode(token):
    if token == NULL:
        return None
    result = []
    i = 0
    while i < len(token):
        if token[i] == '\\':
            if i+1 >= len(token) or token[i+1] != '\\':
                raise ValueError('Invalid TSV escape')
            i += 1
        if token[i] in '\t\r\n\x00':
            raise ValueError('TSV field contains a forbidden control character')
        result.append(token[i])
        i += 1
    return ''.join(result)


def line(values):
    return '\t'.join(encode(value) for value in values)+'\n'


def parse_line(text, fields):
    if not text.endswith('\n') or '\r' in text or '\x00' in text:
        raise ValueError('Invalid TSV line terminator')
    tokens = text[:-1].split('\t')
    if len(tokens) != fields:
        raise ValueError('TSV column count mismatch')
    return [decode(token) for token in tokens]
