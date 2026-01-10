import re
from types_mapper import map_type

def handle_variable_declarations(code: str) -> str:
    lines = code.split('\n')
    result_lines = []
    
    for line in lines:
        # Если строка содержит for(, НЕ обрабатываем её!
        if 'for(' in line or 'for (' in line:
            result_lines.append(line)
            continue
        pattern_init = r'(\w+(?:\s+\w+)*|\w+\*)\s+(\w+)\s*=\s*([^;]+);'
        
        def replace_var_decl_init(match):
            full_type = match.group(1).strip()
            var_name = match.group(2)
            init_value = match.group(3).strip()
            if full_type == 'return':
                return match.group(0)
            
            c_type = ' '.join(full_type.split())
            return f'{var_name} = {init_value}'
        
        line = re.sub(pattern_init, replace_var_decl_init, line)
        
        pattern = r'(\w+(?:\s+\w+)*|\w+\*)\s+(\w+)\s*;'
        
        def replace_var_decl(match):
            full_type = match.group(1).strip()
            var_name = match.group(2)
            
            if full_type in ['return', 'break', 'continue']:
                return match.group(0)
            
            c_type = ' '.join(full_type.split())
            python_type = map_type(c_type)
            
            if python_type == 'int':
                return f'{var_name} = 0'
            elif python_type == 'float':
                return f'{var_name} = 0.0'
            elif python_type == 'str':
                return f'{var_name} = ""'
            elif python_type == 'bool':
                return f'{var_name} = False'
            elif python_type == 'None':
                return f'{var_name} = None'
            else:
                return f'{var_name} = None  # type: {python_type}'
        
        line = re.sub(pattern, replace_var_decl, line)
        result_lines.append(line)
    
    return '\n'.join(result_lines)

def handle_array_declarations(code: str) -> str:
    pattern = r'(\w+)\s+(\w+)\s*\[(\d+)\]\s*;'
    def replace_array_decl(match):
        elem_type = match.group(1)
        arr_name = match.group(2)
        size = match.group(3)
        
        python_type = map_type(elem_type)
        if python_type == 'int':
            return f'{arr_name} = [0] * {size}'
        elif python_type == 'float':
            return f'{arr_name} = [0.0] * {size}'
        else:
            return f'{arr_name} = [None] * {size}'
    
    code = re.sub(pattern, replace_array_decl, code)
    
    pattern_init = r'(\w+)\s+(\w+)\s*\[\s*\]\s*=\s*\{([^}]+)\};'
    def replace_array_init(match):
        elem_type = match.group(1)
        arr_name = match.group(2)
        values = match.group(3)
        
        return f'{arr_name} = [{values}]'
    
    code = re.sub(pattern_init, replace_array_init, code)
    
    return code

def handle_const_declarations(code: str) -> str:
    pattern = r'const\s+(\w+)\s+(\w+)\s*=\s*([^;]+);'
    def replace_const_decl(match):
        c_type = match.group(1)
        var_name = match.group(2)
        value = match.group(3)
        
        return f'{var_name} = {value}  # const'
    
    code = re.sub(pattern, replace_const_decl, code)
    
    return code

def process_declarations(code: str) -> str:
    code = handle_const_declarations(code)
    code = handle_array_declarations(code)
    code = handle_variable_declarations(code)
    return code