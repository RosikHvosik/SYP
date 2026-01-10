import re
from types_mapper import map_type

def handle_structs(code: str) -> str:
    struct_pattern = r'struct\s+(\w+)\s*\{([^}]*?)\}\s*;'
    
    def replace_struct(match):
        struct_name = match.group(1)
        fields_content = match.group(2)
        
        fields = [field.strip() for field in fields_content.split(';') if field.strip()]
        
        result = [f'class {struct_name}:']
        result.append('    def __init__(self):')
        
        if fields:
            for field in fields:
                field = field.strip()
                if field:
                    field_parts = field.split()
                    if len(field_parts) >= 2:
                        field_type = ' '.join(field_parts[:-1])
                        field_name = field_parts[-1].replace('*', '')
                        
                        python_type = map_type(field_type)
                        if python_type == 'int':
                            result.append(f'        self.{field_name} = 0')
                        elif python_type == 'float':
                            result.append(f'        self.{field_name} = 0.0')
                        elif python_type == 'str':
                            result.append(f'        self.{field_name} = ""')
                        elif python_type == 'bool':
                            result.append(f'        self.{field_name} = False')
                        else:
                            result.append(f'        self.{field_name} = None')
        else:
            result.append('        pass')
        
        return '\n'.join(result) + '\n\n'
    
    code = re.sub(struct_pattern, replace_struct, code, flags=re.DOTALL)
    
    struct_usage_pattern = r'struct\s+(\w+)\s+(\w+)\s*;'
    code = re.sub(struct_usage_pattern, r'\2 = \1()', code)

    struct_ptr_pattern = r'struct\s+(\w+)\s*\*\s*(\w+)\s*;'
    code = re.sub(struct_ptr_pattern, r'\2 = \1()', code)
    
    return code

def handle_unions(code: str) -> str:
    union_pattern = r'union\s+(\w+)\s*\{([^}]*?)\}\s*;'
    
    def replace_union(match):
        union_name = match.group(1)
        fields_content = match.group(2)
        
        fields = [field.strip() for field in fields_content.split(';') if field.strip()]
        
        result = [f'class {union_name}:']
        result.append('    def __init__(self):')
        
        if fields:
            for field in fields:
                field = field.strip()
                if field:
                    field_parts = field.split()
                    if len(field_parts) >= 2:
                        field_name = field_parts[-1].replace('*', '')
                        result.append(f'        self.{field_name} = None')
        else:
            result.append('        pass')
        
        return '\n'.join(result) + '\n\n'
    
    code = re.sub(union_pattern, replace_union, code, flags=re.DOTALL)
    return code

def handle_enums(code: str) -> str:
    enum_pattern = r'enum\s+(\w+)\s*\{([^}]*?)\}\s*;'
    
    def replace_enum(match):
        enum_name = match.group(1)
        values_content = match.group(2)
        
        values = [val.strip() for val in values_content.split(',') if val.strip()]
        
        result = [f'class {enum_name}:']
        
        counter = 0
        for value in values:
            value = value.strip()
            if '=' in value:
                name, val = value.split('=')
                name = name.strip()
                val = val.strip()
                result.append(f'    {name} = {val}')
                try:
                    counter = int(val) + 1
                except ValueError:
                    counter += 1
            else:
                result.append(f'    {value} = {counter}')
                counter += 1
        
        return '\n'.join(result) + '\n\n'
    
    code = re.sub(enum_pattern, replace_enum, code, flags=re.DOTALL)
    
    enum_usage_pattern = r'enum\s+(\w+)\s+(\w+)\s*;'
    code = re.sub(enum_usage_pattern, r'\2 = \1()', code)
    
    return code

def process_structures(code: str) -> str:
    code = handle_enums(code)
    code = handle_unions(code)
    code = handle_structs(code)
    return code