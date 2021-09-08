import datetime
import jinja2  # type: ignore
import markdown

from typing import Any


_alphabet = [chr(x) for x in range(ord('a'), ord('z') + 1)]
_alphabet_size = len(_alphabet)
_romans = [(1000, 'M'), (900, 'CM'), (500, 'D'), (400, 'CD'), (100, 'C'), (90, 'XC'),
           (50, 'L'), (40, 'XL'), (10, 'X'), (9, 'IX'), (5, 'V'), (4, 'IV'), (1, 'I')]


def datetime_format(iso_timestamp: str, fmt: str):
    if iso_timestamp is None:
        return ''
    timestamp_stripped = iso_timestamp.split('.')[0]
    return datetime.datetime.strptime(timestamp_stripped, '%Y-%m-%dT%H:%M:%S').strftime(fmt)


def extract(obj, keys):
    return [obj[key] for key in keys if key in obj.keys()]


def of_alphabet(n: int):
    result = ''
    while n >= 0:
        n, m = divmod(n, _alphabet_size)
        result = result + _alphabet[m]
        if n == 0:
            break
    return result


def roman(n: int) -> str:
    result = ''
    while n > 0:
        for i, r in _romans:
            while n >= i:
                result += r
                n -= i
    return result


def xmarkdown(md_text: str):
    if md_text is None:
        return ''
    return jinja2.Markup(markdown.markdown(
        text=md_text,
        extensions=[
            'mdx_breakless_lists',
        ]
    ))


def dot(text: str):
    if text.endswith('.') or len(text.strip()) == 0:
        return text
    return text + '.'


def _has_value(reply: dict) -> bool:
    return bool(reply) and ('value' in reply.keys()) and ('value' in reply['value'].keys())


def _get_value(reply: dict) -> Any:
    return reply['value']['value']


def reply_str_value(reply: dict) -> str:
    if _has_value(reply):
        return str(_get_value(reply))
    return ''


def reply_int_value(reply: dict) -> int:
    if _has_value(reply):
        return int(_get_value(reply))
    return 0


def reply_float_value(reply: dict) -> float:
    if _has_value(reply):
        return float(_get_value(reply))
    return 0


def reply_items(reply: dict) -> list:
    if _has_value(reply) and isinstance(_get_value(reply), list):
        return _get_value(reply)
    return []


def find_reply(replies, path, xtype='string'):
    if isinstance(path, list):
        path = reply_path(path)
    reply = replies.get(path, default=None)
    if not _has_value(reply):
        return None
    r = _get_value(reply)
    if xtype == 'int':
        return r if isinstance(r, int) else int(r)
    if xtype == 'float':
        return r if isinstance(r, float) else float(r)
    if xtype == 'list':
        return r if isinstance(r, list) else list(r)
    return str(r)


def reply_path(uuids: list) -> str:
    return '.'.join(map(str, uuids))


filters = {
    'any': any,
    'all': all,
    'datetime_format': datetime_format,
    'extract': extract,
    'of_alphabet': of_alphabet,
    'roman': roman,
    'markdown': xmarkdown,
    'dot': dot,
    'reply_str_value': reply_str_value,
    'reply_int_value': reply_int_value,
    'reply_float_value': reply_float_value,
    'reply_items': reply_items,
    'find_reply': find_reply,
    'reply_path': reply_path,
}
