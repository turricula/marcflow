import json
import re
import xml.etree.ElementTree as Et
from functools import partial
from io import StringIO


class MarcFlow:
    def __init__(self):
        self._fields = []
        self._conditions = []
        self._combo = ''
        self._dedup = True
        self._json = True
        self._ANY = '_'
        self._IND = '*'

    def select(self, statement=''):
        self._fields = []
        self._conditions = []
        self._combo = ''
        if not isinstance(statement, str):
            return False
        statement = statement.lstrip().replace('\t', ' ')
        if not statement:
            return False
        n = 0
        for token in statement.split():
            if not len(token) in (3, 4):
                break
            n += 1
        tokens = statement.split(maxsplit=n)
        fields = tokens[:n]
        condition = '' if fields == tokens else tokens.pop()
        return self._set_field(fields) and self._set_condition(condition)

    def dedup(self, positive=True):
        self._dedup = positive if isinstance(positive, bool) else True
        return self

    def json(self, positive=True):
        self._json = positive if isinstance(positive, bool) else True
        return self

    def debug(self):
        return {
            'field': self._fields,
            'condition': self._conditions,
            'combo': self._combo,
            'dedup': self._dedup,
            'json': self._json
        }

    def marc(self, source):
        if isinstance(source, str):
            source = StringIO(source)
        try:
            tail = ''
            for chunk in iter(partial(source.read, 4096), ''):
                if len(records := chunk.strip('\r\n').split('\x1D')) <= 1:
                    tail += chunk
                    continue
                yield self._parse_marc(tail + records[0])
                for record in records[1:-1]:
                    yield self._parse_marc(record)
                tail = records[-1]
            if tail:
                yield self._parse_marc(tail)
        except (AttributeError, UnicodeDecodeError):
            yield None

    def marcxml(self, source):
        if isinstance(source, str):
            source = StringIO(source)
        try:
            namespaces = {n[0]: n[1] for _, n in Et.iterparse(
                source, events=['start-ns'])}
            source.seek(0)
            if not (root := Et.parse(source).getroot()):
                yield None
            if any(root.tag == f'{{{n}}}record' for n in namespaces.values()):
                yield self._parse_marcxml(root, namespaces)
            else:
                for record in root.iterfind('record', namespaces):
                    yield self._parse_marcxml(record, namespaces)
        except Et.ParseError:
            yield None

    def aleph(self, source):
        if isinstance(source, str):
            source = StringIO(source)
        try:
            asn = ''
            records = []
            for line in source:
                if len(line := line.strip()) < 19:
                    continue
                if asn and asn != line[:9]:
                    yield self._parse_aleph(records)
                    records = []
                asn = line[:9]
                records.append(line)
            if asn:
                yield self._parse_aleph(records)
        except (TypeError, UnicodeDecodeError):
            yield None

    def _set_field(self, fields=None):
        pattern = re.compile('^\\w{3}[\\w*]?$')
        for f in fields:
            if not f.strip(self._ANY) or not re.match(pattern, f):
                self._fields = []
                return False
            self._fields.append(f.lower())
        return True

    def _set_condition(self, condition=''):
        if not condition:
            return True
        pattern = '(\\w{3}[\\w ]{2}\\w[^ )]*)'
        condition = condition.replace('\\ ', '\0').replace(
            '\\(', '\t').replace('\\)', '\v')
        conditions = []
        for cond in re.findall(pattern, condition):
            if len(cond) < 6 or not cond[:6].strip(self._ANY):
                return False
            if len(cond) == 6:
                regex = None
            else:
                try:
                    regex = re.compile(cond[6:].replace('\0', ' ').replace(
                        '\t', '(').replace('\v', ')'))
                except re.error:
                    return False
            label = cond[:6].lower().replace(self._IND, ' ')
            conditions.append({'label': label, 'regex': regex, 'match': []})
        if not conditions:
            return False
        combo = re.sub(pattern, '{}', condition)
        symbols = (' ', '{', '}', '(', ')', '!', '&', '|')
        if any([c not in symbols for c in combo]):
            return False
        combo = ' '.join(combo.replace('!', ' not ').replace(
            '&', ' and ').replace('|', ' or ').split())
        if combo.count('{}') != len(conditions):
            return False
        try:
            eval(combo, {'__builtins__': None}, None)
        except SyntaxError:
            return False
        self._conditions = conditions
        self._combo = combo
        return True

    def _parse_marc(self, record):
        for condition in self._conditions:
            condition['match'] = []
        if not record:
            return None
        record = record.lstrip().replace('\t', '').replace(
            '\r', '').replace('\n', '')
        if not 40 <= len(record) < 99999:
            return None
        base = record.find('\x1E')
        if base == -1 or base % 12 != 0:
            return None
        if record.count('\x1E') != base / 12 - 1:
            return None
        for i in range(24 + 3, base, 12):
            if not record[i: i + 9].isdigit():
                return None
        values = [[] for _ in range(len(self._fields))]
        self._extract_field('LDR', record[:24], values)
        self._set_match('LDR' + self._ANY * 3, record[:24])
        entries = {
            record[i + 7: i + 12]: record[i: i + 3] for i in range(24, base, 12)
        }
        tags = [tag for _, tag in sorted(entries.items())]
        fields = record[base + 1:].split('\x1E')
        for tag, field in zip(tags, fields):
            if tag.startswith('00'):
                self._extract_field(tag, field, values)
                self._set_match(tag + self._ANY * 3, field)
                continue
            subfields = field.split('\x1F')
            ind = subfields.pop(0)
            self._extract_field(tag, field[2:], values)
            self._extract_field(tag + self._IND, ind, values)
            for sf in subfields:
                if len(sf) > 1:
                    self._extract_field(tag + sf[:1], sf[1:], values)
                    self._set_match(tag + ind + sf[:1], sf[1:])
        return self._get_result(values)

    def _parse_marcxml(self, record, nss):
        for condition in self._conditions:
            condition['match'] = []
        if not record:
            return None
        values = [[] for _ in range(len(self._fields))]
        ldr = record.find('leader', nss)
        if ldr and ldr.text:
            self._extract_field('LDR', ldr.text, values)
            self._set_match('LDR' + self._ANY * 3, ldr.text)
        for cf in record.findall('controlfield', nss):
            tag = cf.attrib.get('tag', None)
            if not tag or not cf.text:
                continue
            if tag != 'LDR' or (ldr and ldr.text != cf.text):
                self._extract_field(tag, cf.text, values)
                self._set_match(tag + self._ANY * 3, cf.text)
        for df in record.findall('datafield', nss):
            if not (tag := df.attrib.get('tag', None)):
                continue
            ind1 = df.attrib.get('ind1', ' ')
            ind2 = df.attrib.get('ind2', ' ')
            if len(tag + ind1 + ind2) != 5:
                continue
            self._extract_field(tag + self._IND, ind1 + ind2, values)
            sfs = []
            for sf in df:
                if (code := sf.attrib.get('code', None)) and sf.text:
                    self._extract_field(tag + code, sf.text, values)
                    self._set_match(tag + ind1 + ind2 + code, sf.text)
                    sfs.append(code + sf.text)
            self._extract_field(tag, '\x1F' + '\x1F'.join(sfs), values)
        return self._get_result(values)

    def _parse_aleph(self, records):
        for condition in self._conditions:
            condition['match'] = []
        if not records:
            return None
        values = [[] for _ in range(len(self._fields))]
        if len(f := records[0].strip()) > 18 and (asn := f[:9]).isdigit():
            self._extract_field('ASN', asn, values)
            self._set_match('ASN' + self._ANY * 3, asn)
        for field in records:
            if len(f := field.strip()) < 19 or not f[:9].isdigit():
                continue
            tag = f[10:13]
            value = f[18:]
            if tag in ('FMT', 'LDR') or tag.startswith('00'):
                self._extract_field(tag, value, values)
                self._set_match(tag + self._ANY * 3, value)
                continue
            ind = f[13:15]
            self._extract_field(tag, value, values)
            self._extract_field(tag + self._IND, ind, values)
            subfields = value.split('$$')
            for sf in subfields:
                if len(sf) > 1:
                    self._extract_field(tag + sf[0], sf[1:], values)
                    self._set_match(tag + ind + sf[0], sf[1:])
        return self._get_result(values)

    def _extract_field(self, label, value, values):
        if not value:
            return
        for i, field in enumerate(self._fields):
            if field == label:
                values[i].append(value)
                continue
            if len(field) != len(label):
                continue
            if field.endswith(self._ANY) and label.endswith(self._IND):
                continue
            for f, l in zip(field, label.lower()):
                if f not in (self._ANY, l):
                    break
            else:
                values[i].append(value)

    def _set_match(self, label, value):
        if not value:
            return
        for condition in self._conditions:
            for f, l in zip(condition['label'], label.lower()):
                if f not in (self._ANY, l):
                    break
            else:
                match = not (r := condition['regex']) or re.search(r, value)
                condition['match'].append(match)

    def _get_result(self, values):
        if self._conditions:
            match = [any(c['match']) for c in self._conditions]
            ex = self._combo.format(*match)
            if not eval(ex, {'__builtins__': None}, None):
                return '{}' if self._json else []
        if not self._json:
            if self._dedup:
                return [list(dict.fromkeys(v)) for v in values]
            return values
        data = {}
        for f, v in zip(self._fields, values):
            if len(v) > 1 and self._dedup:
                v = list(dict.fromkeys(v))
            data[f] = v
        return json.dumps(data, ensure_ascii=False)
