# Marcflow

Marcflow is a Python library for MARC (MAchine-Readable Cataloging) data
preprocessing. It can be used to extract data from records that meet specified
criteria and convert the data to JSON.

## General

```
marcflow.select(statement)
```

Specifies a scheme for parsing MARC records.

**statement** (*str*): field-expr [condition-expr]

- field-expr indicates the (sub)fields whose values will be
  extracted.
- condition-expr, if given, indicates the condition(s) that
  (sub)fields must satisfy to be selected.

Returns True if the parameter is a valid statement and False otherwise.

```
marcpick.marc(source)
```

Parses MARC records and extracts data based on specific criteria.

**source** (*str* | *TextIO*): one or more MARC records

Returns a generator that can be iterated over to obtain the extracted data.

## Installation

```
$ pip install marcflow
```

## Usage

```
>>> from marcflow import MarcFlow
>>> marcflow = MarcFlow()
>>> # The wildcard _ (Low Line) represents any single character in tags,
>>> # indicators and subfield codes.
>>> statement = 'LDR 001 010a 5__a (200__a\(?i\)java & !200__a\(?i\)script) \
... | 606__a^JAVA'
>>> marcflow.select(statement)
True
>>> with open('test.mrc', encoding='UTF-8') as fr:
...     data = marcflow.marc(fr)
...     next(data)
```