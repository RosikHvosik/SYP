import re
from typing import Tuple, List

from preprocessor import preprocess, normalize_code_structure
from utils import (preserve_strings, restore_strings, 
                   preserve_char_literals, restore_char_literals, 
                   normalize_whitespace)
from declarations import process_declarations
from functions import process_functions
from structures import process_structures
from statements import process_statements
from expressions import process_expressions

class CToPythonTranslator:
    def __init__(self):
        self.processing_order = [
            self._preprocess,
            self._handle_structures,
            self._handle_function_decls,
            self._handle_declarations,
            self._handle_statements,
            self._handle_function_calls,
            self._handle_expressions,
            self._handle_syntax,
            self._fix_indentation,
            self._format_code
        ]
    
    def _preprocess(self, code: str) -> str:
        code, self._strings = preserve_strings(code)
        code, self._chars = preserve_char_literals(code)
        code = preprocess(code)
        return code
    
    def _handle_structures(self, code: str) -> str:
        code = process_structures(code)
        lines = code.split('\n')
        normalized = []
        for line in lines:
            stripped = line.lstrip()
            if stripped.startswith(('void ', 'int ', 'float ', 'char ', 'double ', 'long ', 'short ')):
                normalized.append(stripped)
            else:
                normalized.append(line)
        return '\n'.join(normalized)
    
    def _handle_function_decls(self, code: str) -> str:
        from functions import handle_function_declarations
        return handle_function_declarations(code)
    
    def _handle_function_calls(self, code: str) -> str:
        from functions import handle_function_calls, handle_return_statements
        code = handle_function_calls(code)
        code = handle_return_statements(code)
        return code
    
    def _handle_statements(self, code: str) -> str:
        return process_statements(code)
    
    def _handle_declarations(self, code: str) -> str:
        return process_declarations(code)
    
    def _handle_expressions(self, code: str) -> str:
        return process_expressions(code)
    
    def _handle_syntax(self, code: str) -> str:
        code = re.sub(r';', '', code)
        code = re.sub(r'\{', '', code)
        code = re.sub(r'\}', '', code)
        return code
    
    def _fix_indentation(self, code: str) -> str:
        lines = code.split('\n')
        result = []
        indent = 0
        prev_was_block_end = False
        
        for line in lines:
            stripped = line.strip()
            
            if not stripped:
                result.append('')
                continue

            if stripped.startswith(('elif ', 'else:')):
                indent = max(0, indent - 1)
            
            if prev_was_block_end:
                if not stripped.startswith(('elif ', 'else:', 'except:', 'finally:')):
                    indent = max(0, indent - 1)
                prev_was_block_end = False
            
            result.append('    ' * indent + stripped)
            
            if stripped.endswith(':'):
                indent += 1
            
            if stripped.startswith(('return', 'break', 'continue', 'pass')):
                prev_was_block_end = True
        
        return '\n'.join(result)
    
    def _format_code(self, code: str) -> str:
        code = restore_char_literals(code, self._chars)
        code = restore_strings(code, self._strings)
        code = normalize_whitespace(code)
        code = normalize_code_structure(code)
        lines = code.split('\n')
        result = []
        prev_empty = False
        
        for line in lines:
            if not line.strip():
                if not prev_empty:
                    result.append('')
                prev_empty = True
            else:
                result.append(line)
                prev_empty = False
        
        final_lines = []
        for i, line in enumerate(result):
            final_lines.append(line)
            if line.startswith(('class ', 'def ')) and i < len(result) - 1:
                if result[i + 1].strip() and not result[i + 1].startswith('    '):
                    final_lines.append('')
        
        return '\n'.join(final_lines)
    
    def translate(self, c_code: str) -> str:
        code = c_code
        for processor in self.processing_order:
            code = processor(code)
        return code

def translate_c_to_python(c_code: str) -> str:
    translator = CToPythonTranslator()
    return translator.translate(c_code)

if __name__ == "__main__":
    print("=" * 60)
    print("C to Python Translator - ИСПРАВЛЕННАЯ ВЕРСИЯ")
    print("=" * 60)
    
    sample_c_code = """
    #include <stdio.h>
    
    struct Person {
        int age;
        char* name;
    };
    
    void print_person(struct Person* p) {
        printf("Name: %s, Age: %d\\n", p->name, p->age);
    }
    
    int main() {
        struct Person p;
        p.age = 25;
        p.name = "Ivan";
        
        int arr[3];
        arr[0] = 10;
        arr[1] = 20;
        arr[2] = 30;
        
        int i = 0;
        while (i < 3) {
            printf("%d ", arr[i]);
            i++;
        }
        
        if (p.age > 20) {
            printf("\\nOlder than 20\\n");
        } else {
            printf("\\n20 or younger\\n");
        }
        
        print_person(&p);
        
        for(int j = 0; j < 5; j++) {
            printf("%d ", j);
        }
        
        return 0;
    }
    """
    
    print("\n--- C КОД ---")
    print(sample_c_code)
    
    print("\n" + "=" * 60)
    print("--- PYTHON КОД ---")
    print("=" * 60)
    
    python_code = translate_c_to_python(sample_c_code)
    print(python_code)
    
    print("\n" + "=" * 60)
    print("ПРОВЕРКА ИСПРАВЛЕНИЙ:")
    print("=" * 60)
    
    if "print(" in python_code and "printf" not in python_code:
        print("Printf корректно заменен на print")
    else:
        print("Ошибка: printf не заменен")
    
    if "p.name" in python_code and "p->name" not in python_code:
        print("p->name корректно заменен на p.name")
    else:
        print("✗ Ошибка: p->name не заменен")
    
    if "def print_person" in python_code and "def main" in python_code:
        print("Функции корректно объявлены")
    else:
        print("Ошибка: функции не объявлены")
    
    if "for j in range" in python_code:
        print("Цикл for корректно преобразован")
    else:
        print("Ошибка: цикл for не преобразован")
    
    print("\n" + "=" * 60)
    print("Трансляция завершена!")
    print("=" * 60)