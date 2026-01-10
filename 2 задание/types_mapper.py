from typing import Dict, Set

C_TO_PYTHON_TYPES: Dict[str, str] = {
    'int': 'int',
    'float': 'float',
    'double': 'float',
    'char': 'str',
    'char*': 'str',
    'char[]': 'str',
    'void': 'None',
    'bool': 'bool',
    'long': 'int',
    'short': 'int',
    'unsigned int': 'int',
    'unsigned long': 'int',
    'unsigned short': 'int',
    'unsigned char': 'int'
}

BASIC_C_TYPES: Set[str] = {
    'int', 'float', 'double', 'char', 'void', 'bool', 
    'long', 'short', 'unsigned'
}

def map_type(c_type: str) -> str:
    c_type = c_type.strip()
    return C_TO_PYTHON_TYPES.get(c_type, c_type)

def is_basic_type(type_str: str) -> bool:
    return type_str.strip().split()[0] in BASIC_C_TYPES

def extract_type_and_name(declaration: str) -> tuple:
    parts = declaration.strip().split()
    if len(parts) < 2:
        return None, None
    
    type_parts = []
    var_name = None
    
    for i, part in enumerate(parts):
        if '*' in part:
            if part.replace('*', '').strip() and not type_parts:
                type_parts.append(part.replace('*', ''))
                type_parts.append('*')
            elif part == '*':
                type_parts.append('*')
            else:
                var_name = part.replace('*', '')
        elif not type_parts or part in BASIC_C_TYPES or part == '*':
            type_parts.append(part)
        else:
            var_name = part
            break
    
    if not var_name:
        var_name = type_parts.pop()
    
    c_type = ' '.join(type_parts)
    return c_type, var_name