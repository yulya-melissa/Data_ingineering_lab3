# PHP Vacancies Analytics

Анализ вакансий с использованием RustFS/S3, собственного chunk-based MapReduce и SQL в DuckDB.

## Что изменено

- Исходный CSV загружается в `s3://php-vacancies/raw/vacancies.csv`.
- Подготовка читает исходный объект из RustFS чанками, фильтрует PHP-вакансии и создаёт `processed/php_vacancies.csv`.
- Обработанный CSV загружается в RustFS.
- MapReduce повторно читает обработанный объект непосредственно из S3-потока чанками — исходный CSV целиком локально не скачивается.
- DuckDB через расширение `httpfs` выполняет SQL над тем же `s3://.../processed/php_vacancies.csv`.
- CSV-результаты и `profile.json` сохраняются в `results/` локально и загружаются в RustFS в префикс `results/`.
- Верификация сравнивает навыки, пары и тройки и завершает процесс с ошибкой, если есть расхождения.

## 1. Требования

- Docker и Docker Compose
- Python 3.11–3.13
- `uv` (рекомендуется) или `pip`
- исходный датасет в CSV

## 2. Настройка и запуск RustFS

```bash
cp .env.example .env
```

```bash
docker compose up -d
docker compose logs -f rustfs
```

Консоль: <http://localhost:9001>, S3 API: <http://localhost:9000>.

## 3. Установка зависимостей

С `uv`:

```bash
uv sync
```

Или через pip:

```bash
python -m venv .venv
# Windows PowerShell: .venv\Scripts\Activate.ps1
# Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

## 4. Добавление датасета и запуск анализа

Положить исходный файл в `data/vacancies.csv`, затем из корня проекта:

```bash
uv run src/analysis.py
uv run src/verification.py
```

## 5. Основные результаты

Локально в `results/`
