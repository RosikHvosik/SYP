import re

def handle_struct_pointer_access(code: str) -> str:
    code = re.sub(r'(\w+)->(\w+)', r'\1.\2', code)
    return code

def handle_operators(code: str) -> str:
    code = re.sub(r'(\w+)\+\+', r'\1 += 1', code)
    code = re.sub(r'\+\+(\w+)', r'\1 += 1', code)
    code = re.sub(r'(\w+)--', r'\1 -= 1', code)
    code = re.sub(r'--(\w+)', r'\1 -= 1', code)
    
    code = re.sub(r'\|\|', ' or ', code)
    code = re.sub(r'&&', ' and ', code)
    
    code = re.sub(r'!(\w+)', r'not \1', code)
    code = re.sub(r'!\(([^)]+)\)', r'not (\1)', code)
    
    return code

def handle_pointers(code: str) -> str:
    code = re.sub(r'&(\w+)', r'\1', code)
    
    code = re.sub(r'\*(\w+)\s*=', r'\1 =', code)
    
    return code

def handle_null(code: str) -> str:
    code = re.sub(r'\bNULL\b', 'None', code)
    return code

def normalize_spacing(code: str) -> str:
    code = re.sub(r'([^=!<>])=([^=])', r'\1 = \2', code)
    
    code = re.sub(r'([^=!<>])(<|>)([^=])', r'\1 \2 \3', code)
    code = re.sub(r'\s+', ' ', code)
    
    return code

def process_expressions(code: str) -> str:
    code = handle_struct_pointer_access(code)
    
    code = handle_null(code)

    code = handle_pointers(code)
    
    code = handle_operators(code)

    return code