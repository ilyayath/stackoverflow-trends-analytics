import base64

# Переконайся, що файл називається new_credentials.json і лежить поруч
filename = 'credentials.json'

try:
    with open(filename, 'rb') as f:
        key_content = f.read()

    # Кодуємо в base64
    encoded_key = base64.b64encode(key_content).decode('utf-8')

    print("\nУСПІХ! СКОПІЮЙ ЦЕЙ РЯДОК НИЖЧЕ ДЛЯ GITHUB SECRETS:\n")
    print(encoded_key)
    print("\n")
except FileNotFoundError:
    print(f"Помилка: Не знайшов файл {filename}. Перевір назву!")