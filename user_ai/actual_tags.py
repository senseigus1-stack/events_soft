import pandas as pd
import re


def process_csv_extract_unique_tags(input_csv_path, output_csv_path=None):

    df = pd.read_csv(input_csv_path)
    
    all_tags = []
    
    # Проходим по каждой строке в столбце с тегами
    for line in df.iloc[:, 0]:  # берём первый столбец
        if pd.isna(line):  # пропускаем пустые строки
            continue
        
        # 1. Удаляем "", {}, и лишние пробелы
        cleaned = re.sub(r'["{}]', '', str(line)).strip()
        
        # 2. Разбиваем по запятым, чистим каждый тег
        tags = [tag.strip() for tag in cleaned.split(',') if tag.strip()]
        all_tags.extend(tags)
    
    
    # 3. Получаем уникальные теги
    unique_tags = sorted(list(set(all_tags)))  # сортируем для удобства
    
    
    # 4. Сохраняем в CSV (если указан путь)
    if output_csv_path:
        result_df = pd.DataFrame({'unique_tag': unique_tags})
        result_df.to_csv(output_csv_path, index=False, encoding='utf-8')
        print(f"Уникальные теги сохранены в {output_csv_path}")
    
    
    return unique_tags

input_path = 'C:/Users/redmi/events_soft/user_ai/tags.csv'           # входной CSV
output_path = 'C:/Users/redmi/events_soft/user_ai/tags_unique.csv' # куда хранить результат (можно None)


unique_tags = process_csv_extract_unique_tags(input_path, output_path)

print("Уникальные теги:")
for tag in unique_tags:
    print(tag)