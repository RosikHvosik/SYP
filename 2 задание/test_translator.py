from main import translate_c_to_python

def test_basic():
    """Тест 1: Базовая программа"""
    print("\n" + "=" * 60)
    print("ТЕСТ 1: Базовая программа")
    print("=" * 60)
    
    c_code = """
    int main() {
        int x = 5;
        return 0;
    }
    """
    
    expected_contains = [
        "def main():",
        "x = 5",
        "return"
    ]
    
    result = translate_c_to_python(c_code)
    print("Результат:")
    print(result)
    
    for expected in expected_contains:
        if expected in result:
            print(f"✓ Найдено: {expected}")
        else:
            print(f"✗ НЕ найдено: {expected}")
    
    return result

def test_struct():
    """Тест 2: Структуры"""
    print("\n" + "=" * 60)
    print("ТЕСТ 2: Структуры")
    print("=" * 60)
    
    c_code = """
    struct Person {
        int age;
        char* name;
    };
    
    struct Person p;
    p.age = 25;
    """
    
    expected_contains = [
        "class Person:",
        "def __init__(self):",
        "self.age",
        "self.name",
        "p = Person()",
        "p.age = 25"
    ]
    
    result = translate_c_to_python(c_code)
    print("Результат:")
    print(result)
    
    success = 0
    for expected in expected_contains:
        if expected in result:
            print(f"✓ Найдено: {expected}")
            success += 1
        else:
            print(f"✗ НЕ найдено: {expected}")
    
    print(f"\nУспех: {success}/{len(expected_contains)}")
    return result

def test_loops():
    """Тест 3: Циклы"""
    print("\n" + "=" * 60)
    print("ТЕСТ 3: Циклы")
    print("=" * 60)
    
    c_code = """
    int main() {
        for(int i = 0; i < 5; i++) {
            printf("%d", i);
        }
        
        int j = 0;
        while(j < 3) {
            j++;
        }
        
        return 0;
    }
    """
    
    expected_contains = [
        "def main():",
        "for i in range(0, 5):",
        "print",
        "j = 0",
        "while j < 3:",
        "j += 1",
        "return"
    ]
    
    result = translate_c_to_python(c_code)
    print("Результат:")
    print(result)
    
    success = 0
    for expected in expected_contains:
        if expected in result:
            print(f"✓ Найдено: {expected}")
            success += 1
        else:
            print(f"✗ НЕ найдено: {expected}")
    
    print(f"\nУспех: {success}/{len(expected_contains)}")
    return result

def test_conditionals():
    """Тест 4: Условия"""
    print("\n" + "=" * 60)
    print("ТЕСТ 4: Условия")
    print("=" * 60)
    
    c_code = """
    int main() {
        int x = 10;
        if (x > 5) {
            printf("big");
        } else {
            printf("small");
        }
        return 0;
    }
    """
    
    expected_contains = [
        "if x > 5:",
        "print",
        "else:",
    ]
    
    result = translate_c_to_python(c_code)
    print("Результат:")
    print(result)
    
    success = 0
    for expected in expected_contains:
        if expected in result:
            print(f"✓ Найдено: {expected}")
            success += 1
        else:
            print(f"✗ НЕ найдено: {expected}")
    
    print(f"\nУспех: {success}/{len(expected_contains)}")
    return result

def test_pointer_access():
    """Тест 5: Доступ через указатель (КРИТИЧНЫЙ ТЕСТ)"""
    print("\n" + "=" * 60)
    print("ТЕСТ 5: p->field (КРИТИЧНЫЙ)")
    print("=" * 60)
    
    c_code = """
    struct Node {
        int data;
    };
    
    void func(struct Node* ptr) {
        ptr->data = 42;
    }
    """
    
    result = translate_c_to_python(c_code)
    print("Результат:")
    print(result)
    
    if "ptr.data" in result and "ptr->data" not in result:
        print("✓ УСПЕХ: ptr->data преобразовано в ptr.data")
        return True
    else:
        print("✗ ОШИБКА: ptr->data НЕ преобразовано!")
        if "ptr->data" in result:
            print("  Найдено: ptr->data (не должно быть)")
        if "ptr.data" not in result:
            print("  НЕ найдено: ptr.data (должно быть)")
        return False

def test_full_program():
    """Тест 6: Полная программа из задания"""
    print("\n" + "=" * 60)
    print("ТЕСТ 6: Полная программа")
    print("=" * 60)
    
    c_code = """
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
        
        int i = 0;
        while (i < 3) {
            printf("%d ", arr[i]);
            i++;
        }
        
        if (p.age > 20) {
            printf("Older than 20");
        }
        
        print_person(&p);
        
        for(int j = 0; j < 5; j++) {
            printf("%d ", j);
        }
        
        return 0;
    }
    """
    
    result = translate_c_to_python(c_code)
    print("Результат:")
    print(result)
    
    # Проверяем критичные элементы
    checks = {
        "Класс Person": "class Person:" in result,
        "Функция print_person": "def print_person" in result,
        "Функция main": "def main():" in result,
        "p->name стал p.name": "p.name" in result and "p->name" not in result,
        "p->age стал p.age": "p.age" in result and "p->age" not in result,
        "Цикл for": "for j in range" in result,
        "Цикл while": "while i < 3:" in result,
        "Условие if": "if p.age > 20:" in result,
        "Массив": "arr = [" in result or "arr[0]" in result,
        "Нет фигурных скобок": "{" not in result and "}" not in result,
        "Нет printf": "printf" not in result,
    }
    
    print("\n" + "=" * 60)
    print("Проверка критичных элементов:")
    print("=" * 60)
    
    success = 0
    total = len(checks)
    
    for name, passed in checks.items():
        status = "✓" if passed else "✗"
        print(f"{status} {name}")
        if passed:
            success += 1
    
    print(f"\nИтого: {success}/{total} ({success*100//total}%)")
    
    return result

if __name__ == "__main__":
    print("=" * 60)
    print("ТЕСТИРОВАНИЕ ТРАНСЛЯТОРА C → PYTHON")
    print("=" * 60)
    
    # Запускаем все тесты
    test_basic()
    test_struct()
    test_loops()
    test_conditionals()
    test_pointer_access()
    test_full_program()
    
    print("\n" + "=" * 60)
    print("ВСЕ ТЕСТЫ ЗАВЕРШЕНЫ")
    print("=" * 60)