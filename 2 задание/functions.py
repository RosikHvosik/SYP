import re
from typing import List

def handle_function_declarations(code: str) -> str:
    pattern = r'(\w+(?:\s+\w+)*)\s+(\w+)\s*\(([^)]*)\)\s*\{'
    
    def replace_function(match):
        return_type_with_qualifiers = match.group(1).strip()
        func_name = match.group(2)
        params = match.group(3).strip()
        python_params = []
        if params and params != 'void':
            param_list = [p.strip() for p in params.split(',')]
            for param in param_list:
                if param:
                    param_parts = param.split()
                    if len(param_parts) >= 2:
                        param_name = param_parts[-1].replace('*', '').replace('[', '').replace(']', '')
                        python_params.append(param_name)
                    else:
                        python_params.append(param_parts[-1].replace('*', ''))
        
        params_str = ', '.join(python_params)
        return f'def {func_name}({params_str}):'
    
    code = re.sub(pattern, replace_function, code)
    return code

def handle_function_calls(code: str) -> str:
    printf_pattern1 = r'printf\s*\(([^)]+)\)\s*;'
    code = re.sub(printf_pattern1, r'print(\1)', code)
    
    printf_pattern2 = r'printf\s*\(([^)]+)\)'
    code = re.sub(printf_pattern2, r'print(\1)', code)
    
    replacements = [
        (r'scanf\s*\(', '# scanf - use input(): '),
        (r'gets\s*\(', 'input('),
        (r'puts\s*\(', 'print('),
        (r'strlen\s*\(', 'len('),
        (r'malloc\s*\(', '# malloc - use list or dict: '),
        (r'free\s*\(', '# free - automatic in Python: '),
        (r'sizeof\s*\(', '# sizeof - use sys.getsizeof(): '),
    ]
    
    for old, new in replacements:
        code = re.sub(old, new, code)
    
    return code

def handle_return_statements(code: str) -> str:
    code = re.sub(r'return\s+([^;]+);', r'return \1', code)
    code = re.sub(r'return\s*;', r'return None', code)
    return code

def process_functions(code: str) -> str:
    code = handle_function_calls(code)
    code = handle_return_statements(code)
    code = handle_function_declarations(code)
    return code