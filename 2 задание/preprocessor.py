import re
from typing import Tuple

def remove_includes(code: str) -> str:
    return re.sub(r'#include\s+<.*?>', '', code)

def remove_defines(code: str) -> str:
    return re.sub(r'#define\s+\w+\s+.*?(?=\n|$)', '', code, flags=re.MULTILINE)

def remove_macros(code: str) -> str:
    code = re.sub(r'#\s*ifdef\s+.*?(?=\n)', '', code, flags=re.MULTILINE)
    code = re.sub(r'#\s*ifndef\s+.*?(?=\n)', '', code, flags=re.MULTILINE)
    code = re.sub(r'#\s*endif\s*(?=\n)', '', code, flags=re.MULTILINE)
    code = re.sub(r'#\s*else\s*(?=\n)', '', code, flags=re.MULTILINE)
    code = re.sub(r'#\s*elif\s+.*?(?=\n)', '', code, flags=re.MULTILINE)
    return code

def remove_comments(code: str) -> str:
    code = re.sub(r'//.*?$', '', code, flags=re.MULTILINE)
    code = re.sub(r'/\*.*?\*/', '', code, flags=re.DOTALL)
    return code

def preprocess(code: str) -> str:
    code = remove_includes(code)
    code = remove_defines(code)
    code = remove_macros(code)
    code = remove_comments(code)
    return code.strip()

def normalize_code_structure(code: str) -> str:
    code = re.sub(r'\n\s*\n\s*\n', '\n\n', code)
    code = code.strip()
    return code