# https://learn.microsoft.com/en-us/azure/data-factory/concepts-data-flow-expression-builder
# https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form
# https://docs.python.org/3/reference/grammar.html


# PowerShell-like syntax (at least for arrays)


import re
import xml.etree.ElementTree as ET
from itertools import chain
from typing import NamedTuple


tokens = {
    "operator": {
        "arrow": "->",
        "assign": ":=",
        "add": "+",
        "minus": "-",
        "divide": "/",
        "multiply": "*",
        "mod": "%",
        "and": "&&",
        "or": "||",
        "xor": "^",  # not "^^"?
        "bitwiseAnd": "&",
        "bitwiseOr": "|",
        "bitwiseXor": "^",  # "|" in docs
        "equals": "=",
        "notEquals": "!=",
        "equalsIgnoreCase": "<=>",
        "greaterOrEqual": ">=",
        "lessOrEqual": "<=",
        "least": "<=",
        "greater": ">",
        "lesser": "<",
        "concat": "+",
    },
    "open": {
        "larray": "@(",
        "lparen": "(",
        "lcurly": "{",
        "lsquare": "[",
    },
    "close": {
        "rparen": ")",
        "rcurly": "}",
        "rsquare": "]",
    },
    "sep": {
        "comma": ",",
    },
    "string": {
        "apostrophe": "'",
        "quotes": '"',
        "escape": '\\',
    },
    "space": {
        "space": "re:\s+",
        # "newline": "\n",
    },
    # "end": {
    #     "end": "re:$",
    # },
    "number": {
        "number": "re:(?:\d+|\d*\.\d+)",
    },
    "word": {
        "item": "re:#item(?:_\d+)?",
        "index": "re:#index(?:_\d+)?",
        "word": "re:[a-zA-Z_\\\\]+",
    },
}

### --- Tokenizer

# Named groups must be valid Python identifiers (str.isidentifier)
def item(t, k, v): return f"(?P<{t}__{k}>{v})"
def _pattern(s): return s[3:] if s.startswith("re:") else re.escape(s)


class TokenInfo(NamedTuple):
    type: str
    variant: str
    string: str
    start: int
    end: int
    line: int


def token_pattern(tokens):
    pattern = []
    for type_, type_tokens in tokens.items():
        for variant, variant_pattern in type_tokens.items():
            group = item(type_, variant, _pattern(variant_pattern))
            pattern.append(group)

    pattern = "|".join(pattern)
    pattern = re.compile(pattern)
    return pattern


def tokenize(lines, pattern):
    for lineno, line in enumerate(lines.splitlines(), start=1):
        i = 0
        while m := pattern.match(line, i):
            type_, variant = m.lastgroup.split("__")
            yield TokenInfo(
                type_,
                variant,
                m[m.lastgroup],
                m.start(),
                m.end(),
                lineno,
            )
            i = m.end(m.lastgroup)
        assert i == len(line)

### --- Nodes

def Text(text):
    el = ET.Element(str)
    el.text = text
    return el


class Element(ET.Element):
    def __repr__(self):
        return f"<{self._name()} />"

    def _name(self):
        name = [self.tag]
        for key, value in self.attrib.items():
            name.append(f'{key}="{value}"')
        return " ".join(name)


class Node(Element):
    def __init__(self, tag, text=None, attrib={}, **extra):
        super().__init__(tag, attrib, **extra)
        # self.text = text
        self.set("value", text)
        # self.append(Text(text))

    def __repr__(self):
        return f"<{self._name()}>{self[0].text}</{self.tag}>"


class Collection(Element):
    def __init__(self, tag, children=(), attrib={}, **extra):
        super().__init__(tag, attrib, **extra)
        self.extend(children)

    def __repr__(self):
        return f"<{self._name()} with {len(self)} children>"


### --- Types

def number(string):
    try:
        return int(string)
    except ValueError:
        return float(string)


class TokenError(ValueError):
    pass


### --- Parser

# Lots of room for refactoring here; much is duplicated

def get(tokens, i):
    for j, token in enumerate(tokens[i:], start=i):
        if token.type == "space":
            continue
        return j+1, token
    return -1, TokenInfo("end", "end", "", i, i, 1)


def _i(tokens, i):
    for j, token in enumerate(tokens[i:], start=i):
        if token.type == "space":
            continue
        return j+1
    return j


def get_token(tokens, i):
    for token in tokens[i:]:
        if token.type == "space":
            continue
        return token


def parse_collection(tokens, i):
    arguments = []
    while get_token(tokens, i).type != "close":
        i, expr = parse_function_expression(tokens, i)
        arguments.append(expr)
        j, token = get(tokens, i)
        if token.variant == "comma":
            i = j

    return _i(tokens, i), arguments


def parse_string(tokens, i):
    start = tokens[i-1].variant
    escape = False
    for j, token in enumerate(tokens[i:], start=i):
        if escape:
            escape = False
            continue
        if token.variant == start:
            return j+1, Node("STR", "".join(token.string for token in tokens[i:j]))
        if token.variant == "escape":
            escape = True


def parse_term(tokens, i):
    i, token = get(tokens, i)

    if token.type == "word":
        return i, Node("VAR", token.string)
    elif token.type == "open":
        i, coll = parse_collection(tokens, i)
        if token.variant == "lsquare":
            return i, Collection("LIST", coll)
        elif token.variant == "lparen":
            # This is a 1-child node and can be omitted
            return i, Collection("PAREN", coll)
        else:
            raise NotImplementedError(f"Not implemented '{token.variant}'")
    elif token.type == "number":
        return i, Node("NUM", number(token.string))
    elif token.type == "string":
        i, string = parse_string(tokens, i)
        return i, string
    elif token.type == "operator":
        if token.variant in {"add", "minus"}:
            i, arg = parse_term(tokens, i)
            return i, Collection("UNOP", [Node("OP", token.string), arg])
        else:
            raise NotImplementedError(f"Not implemented '{token.variant}'")
    else:
        raise TokenError(f"Invalid expression at {token}")


def parse_post_expression(tokens, i):
    i, left = parse_term(tokens, i)

    while True:
        j, right = get(tokens, i)

        if right.type == "open":
            i, coll = parse_collection(tokens, j)
            if right.variant == "lparen":
                left = Collection("CALL", [left, Collection("ARGS", coll)])
            elif right.variant == "lsquare":
                left = Collection("GET", [left, Collection("KEY", coll)])
            else:
                raise NotImplementedError(f"Not implemented '{right.variant}'")
        else:
            expr = left
            break

    return i, expr


def parse_prod_expression(tokens, i):
    i, left = parse_post_expression(tokens, i)

    while True:
        j, mid = get(tokens, i)

        if mid.variant in {"multiply", "divide"}:
            i, right = parse_post_expression(tokens, j)
            left = Collection("PRODOP", [left, Node("OP", mid.string), right])
        else:
            expr = left
            break

    return i, expr


def parse_sum_expression(tokens, i):
    i, left = parse_prod_expression(tokens, i)

    while True:
        j, mid = get(tokens, i)

        if mid.variant in {"add", "minus"}:
            i, right = parse_prod_expression(tokens, j)
            left = Collection("SUMOP", [left, Node("OP", mid.string), right])
        else:
            expr = left
            break

    return i, expr


def parse_comp_expression(tokens, i):
    i, left = parse_sum_expression(tokens, i)

    while True:
        j, mid = get(tokens, i)

        if mid.variant in {"equals", "notEquals", "greater", "lesser"}:
            i, right = parse_prod_expression(tokens, j)
            left = Collection("COMPARE", [left, Node("OP", mid.string), right])
        else:
            expr = left
            break

    return i, expr


def parse_logical_expression(tokens, i):
    i, left = parse_comp_expression(tokens, i)

    while True:
        j, mid = get(tokens, i)

        if mid.variant in {"and", "or"}:
            i, right = parse_comp_expression(tokens, j)
            left = Collection("LOGICAL", [left, Node("OP", mid.string), right])
        else:
            expr = left
            break

    return i, expr


def parse_function_expression(tokens, i):
    i, left = parse_logical_expression(tokens, i)
    j, mid = get(tokens, i)

    if mid.variant == "arrow":
        i, right = parse_logical_expression(tokens, j)
        return i, Collection("FUNC", [left, right])
    else:
        return i, left


def parse_expression(tokens, i):
    i, left = parse_logical_expression(tokens, i)
    j, mid = get(tokens, i)

    if mid.variant == "assign":
        i, right = parse_logical_expression(tokens, j)
        return i, Collection("ASSIGN", [left, right])
    else:
        return i, left


def parse(text, pattern):
    tokens = list(tokenize(text, pattern))

    return parse_expression(tokens, 0)[1]


### --- Formatter

import textwrap

font_code = {
    'roman'         : 0,
    'bold'          : 1,
    'italic'        : 3,
    'underline'     : 4,
    'blink'         : 5,
    'mark'          : 7,
    'strikethrough' : 9,
    'default'       : 0,
}

color_code = {
    'black'       : 30,
    'darkred'     : 31,
    'darkgreen'   : 32,
    'darkyellow'  : 33,
    'blue'        : 34,
    'darkmagenta' : 35,
    'darkcyan'    : 36,
    'lightgrey'   : 37, 'lightgray' : 37,
    'grey'        : 38, 'gray'      : 38,
    'darkgrey'    : 90, 'darkgray'  : 90,
    'red'         : 91,
    'green'       : 92,
    'yellow'      : 93,
    'violet'      : 94,
    'magenta'     : 95,
    'cyan'        : 96,
    'white'       : 97,
    'default'     : 38,
}

background_colors = {
    'none'    : 40,
    'black'   : 40,
    'red'     : 41,
    'green'   : 42,
    'yellow'  : 43,
    'blue'    : 44,
    'magenta' : 45,
    'cyan'    : 46,
    'white'   : 47,
}


def tag(codes): return f"\033[{codes}m"


END = tag(0)


def code_style(string, codes):
    codes = ";".join(map(str, filter(None, codes)))
    return f"{tag(codes)}{string}{END}"

def style(string, **codes):
    # ["font", "color", "bg"] (unordered)
    font = font_code.get(codes.get("font"))
    color = color_code.get(codes.get("color"))
    bg = background_colors.get(codes.get("bg"))

    return code_style(string, (color, font, bg))


VAR_STYLE = {"color": "yellow", "font": "bold"}
NUM_STYLE = {"color": "magenta"}
STR_STYLE = {"color": "green"}


def _indent(string, n):
    return textwrap.indent(string, n*" ")


def froot(el, indent=False):
    if False:  # for alignment
        pass
    elif el.tag == "UNOP":
        return "".join(froot(c, indent) for c in el)
    elif el.tag == "PRODOP":
        return "".join(froot(c, indent) for c in el)
    elif el.tag == "SUMOP":
        return " ".join(froot(c, indent) for c in el)
    elif el.tag == "COMPARE":
        return " ".join(froot(c, indent) for c in el)
    elif el.tag == "FUNC":
        return froot(el[0], indent) + " -> " + froot(el[1], indent)
    elif el.tag == "ASSIGN":
        return froot(el[0], indent) + " := " + froot(el[1], indent)
    elif el.tag == "OP":
        return el.get("value")
    elif el.tag == "OP":
        return el.get("value")
    elif el.tag == "CALL":
        if indent is False:
            return froot(el[0]) + "(" + froot(el[1]) + ")"
        else:
            body = froot(el[1], indent)
            return froot(el[0], indent) + "(\n" + _indent(body, indent) + "\n)"
    elif el.tag == "GET":
        return froot(el[0], indent) + "[" + froot(el[1], indent) + "]"
    elif el.tag == "PAREN":
        return "(" + froot(el[0], indent) + ")"
    elif el.tag == "LIST":
        if indent is False:
            return "[" + ", ".join(froot(c, indent) for c in el) + "]"
        else:
            body = ",\n".join(froot(c, indent) for c in el)
            return "[\n" + _indent(body, indent) + ",\n]"
    elif el.tag == "ARGS":
        if indent is False:
            return ", ".join(froot(c, indent) for c in el)
        else:
            return ",\n".join(froot(c, indent) for c in el) + ","
    elif el.tag == "KEY":
        return ", ".join(froot(c, indent) for c in el)
    elif el.tag == "NUM":
        return style(str(el.get("value")), **NUM_STYLE)
    elif el.tag == "VAR":
        return style(el.get("value"), **VAR_STYLE)
    elif el.tag == "STR":
        return style("'" + el.get("value") + "'", **STR_STYLE)
    else:
        raise TypeError(f"Invalid type {el.tag}")


def fprint(root):
    from xml.dom import minidom
    for el in root.iter():
        if value := el.get("value"):
            el.set("value", str(value))
    xmlstr = minidom.parseString(ET.tostring(root)).toprettyxml(indent="   ")
    print(xmlstr)


### --- Evaluator

def root_eval():
    pass


### --- Main

def main():
    tests = [
        r"""-1+hello(
    1,
    'w\"orld',
    3,
)*2-(1+1)""",
        r"split(Player, '\\')[1]",
        r"mapIf(['icecream', 'cake', 'soda'], length(#item)>4, upper(#item))",  # -> ['ICECREAM', 'CAKE']"  # clearly wrong???
        r"['fruit' ->   'apple',  'vegetable' -> 'carrot']",
        r"x := [y -> 2, z->3]",
    ]

    pattern = token_pattern(tokens)

    for test in tests:
        root = parse(test, pattern)
        formatted = froot(root, indent=False)

        print(end="\n\n")
        print(test)
        print(formatted)


if __name__ == "__main__":
    main()

