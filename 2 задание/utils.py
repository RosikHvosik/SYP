import re
from typing import List, Tuple

def preserve_strings(code: str) -> Tuple[str, List[str]]:
    strings = []
    def replace(match):
        strings.append(match.group(0))
        return f"__STRING_{len(strings)-1}__"
    
    preserved_code = re.sub(r'"([^"\\]|\\.)*"', replace, code)
    return preserved_code, strings

def restore_strings(code: str, strings: List[str]) -> str:
    result = code
    for i, s in enumerate(strings):
        result = result.replace(f"__STRING_{i}__", s)
    return result

def preserve_char_literals(code: str) -> Tuple[str, List[str]]:
    chars = []
    def replace(match):
        chars.append(match.group(0))
        return f"__CHAR_{len(chars)-1}__"
    
    preserved_code = re.sub(r"'([^'\\]|\\.)'", replace, code)
    return preserved_code, chars

def restore_char_literals(code: str, chars: List[str]) -> str:
    result = code
    for i, c in enumerate(chars):
        result = result.replace(f"__CHAR_{i}__", c)
    return result

def normalize_whitespace(code: str) -> str:
    lines = code.split('\n')
    normalized_lines = []
    for line in lines:
        normalized_lines.append(line.rstrip())
    return '\n'.join(normalized_lines)