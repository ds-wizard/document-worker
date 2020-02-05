import datetime
import jinja2
import markdown2

_alphabet = [chr(x) for x in range(ord('a'), ord('z')+1)]
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


def markdown(md_text: str):
    if md_text is None:
        return ''
    return jinja2.Markup(markdown2.markdown(md_text))


def dot(text: str):
    if text.endswith('.') or len(text.strip()) == 0:
        return text
    return text + '.'


def reply_str_value(reply) -> str:
    if reply and 'value' in reply and 'value' in reply['value']:
        return reply['value']['value']
    return ''


def reply_int_value(reply) -> int:
    if reply and 'value' in reply and 'value' in reply['value']:
        return int(reply['value']['value'])
    return 0


def reply_path(uuids: list) -> str:
    return '.'.join(map(str, uuids))


filters = {
    'any': any,
    'all': all,
    'datetime_format': datetime_format,
    'extract': extract,
    'of_alphabet': of_alphabet,
    'roman': roman,
    'markdown': markdown,
    'dot': dot,
    'reply_str_value': reply_str_value,
    'reply_int_value': reply_int_value,
    'reply_path': reply_path,
}
