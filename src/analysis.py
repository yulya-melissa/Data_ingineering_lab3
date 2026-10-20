"""Анализ PHP-вакансий: подготовка, потоковый MapReduce, DuckDB и профилирование."""
from __future__ import annotations

import itertools
import json
import os
import re
import statistics
import time
import tracemalloc
from collections import Counter, defaultdict
from pathlib import Path

import duckdb
import pandas as pd
import psutil
from dotenv import load_dotenv

from storage import bucket_name, download_file, get_s3_client, object_uri, open_object, upload_file

load_dotenv()
ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = ROOT / "data" / "vacancies.csv"
RESULTS = ROOT / "results"
SQL_FILE = ROOT / "sql" / "analysis.sql"
RESULTS.mkdir(parents=True, exist_ok=True)
CHUNK_SIZE = max(1, int(os.getenv("CHUNK_SIZE", "5000")))
MIN_COMBO_COUNT = max(1, int(os.getenv("MIN_COMBO_COUNT", "10")))
RAW_KEY = "raw/vacancies.csv"
PROCESSED_KEY = "processed/php_vacancies.csv"


def parse_skills(value) -> list[str]:
    if value is None:
        return []
    skills = []
    for item in str(value).split(";"):
        skill = re.sub(r"\s+", " ", item).strip()
        if skill:
            skills.append(skill)
    # Убираем дубликаты без учёта регистра и задаём общий порядок для комбинаций.
    unique = {skill.casefold(): skill for skill in skills}
    return sorted(unique.values(), key=lambda value: (value.casefold(), value))


def is_php_vacancy(row) -> bool:
    name = str(row.get("name", ""))
    role = str(row.get("role", ""))
    skills = parse_skills(row.get("key_skills", ""))
    if re.search(r"\bphp\b", name, re.IGNORECASE):
        return True
    developer_role = re.search(
        r"программист|разработчик|developer|backend", f"{name} {role}", re.IGNORECASE
    )
    return bool(developer_role and any(s.casefold() == "php" for s in skills))


def get_salary(row):
    currency = str(row.get("currency", "")).strip().upper()
    if currency not in {"RUR", "RUB"}:
        return None
    try:
        low = float(row["salary_from"]) if row.get("salary_from") not in (None, "") else None
        high = float(row["salary_to"]) if row.get("salary_to") not in (None, "") else None
    except (ValueError, TypeError, KeyError):
        return None
    if low is not None and high is not None:
        return (low + high) / 2
    return low if low is not None else high


def prepare_data_from_rustfs(object_key: str = RAW_KEY) -> tuple[Path, dict]:
    """Читает исходный объект чанками из RustFS, готовый CSV сохраняет локально и в RustFS."""
    output = RESULTS / "php_vacancies.csv"
    output.unlink(missing_ok=True)
    total = selected = 0
    wrote_header = False
    started = time.perf_counter()
    body = open_object(object_key)
    try:
        for chunk in pd.read_csv(body, chunksize=CHUNK_SIZE, dtype=str,
                                 keep_default_na=False, encoding="utf-8-sig", on_bad_lines="warn"):
            required = {"vacancy_id", "name", "role", "area", "key_skills"}
            missing = required - set(chunk.columns)
            if missing:
                raise ValueError(f"В CSV отсутствуют обязательные столбцы: {sorted(missing)}")
            total += len(chunk)
            mask = chunk.apply(is_php_vacancy, axis=1)
            php = chunk.loc[mask].copy()
            if php.empty:
                continue
            selected += len(php)
            php["skills_json"] = php["key_skills"].apply(
                lambda value: json.dumps(parse_skills(value), ensure_ascii=False)
            )
            php["salary_mid"] = php.apply(get_salary, axis=1)
            php.to_csv(output, mode="a" if wrote_header else "w", index=False,
                       header=not wrote_header, encoding="utf-8")
            wrote_header = True
    finally:
        body.close()
    if not wrote_header:
        raise ValueError("После фильтрации не найдено PHP-вакансок; проверьте CSV и критерии.")
    elapsed = time.perf_counter() - started
    upload_file(output, PROCESSED_KEY)
    stats = {"total_rows": total, "php_vacancies": selected, "preparation_seconds": round(elapsed, 4)}
    print(f"Исходных строк: {total:,}; PHP-вакансий: {selected:,}; подготовка: {elapsed:.2f} с")
    return output, stats


def save_counter(counter: Counter, columns: list[str], filename: str) -> Path:
    rows = []
    for combo, frequency in counter.items():
        combo = combo if isinstance(combo, tuple) else (combo,)
        row = dict(zip(columns, combo))
        row["frequency"] = frequency
        rows.append(row)
    result = pd.DataFrame(rows, columns=columns + ["frequency"])
    if not result.empty:
        result = result.sort_values("frequency", ascending=False, kind="stable")
    path = RESULTS / filename
    result.to_csv(path, index=False, encoding="utf-8")
    return path


def run_mapreduce_from_rustfs(object_key: str = PROCESSED_KEY) -> dict:
    """MapReduce: получает объект из S3 и обрабатывает его pandas-чанками без полной загрузки CSV."""
    started = time.perf_counter()
    process = psutil.Process()
    rss_before = process.memory_info().rss
    skill_counts, pair_counts, triple_counts = Counter(), Counter(), Counter()
    tracemalloc.start()
    body = open_object(object_key)
    try:
        for chunk in pd.read_csv(body, chunksize=CHUNK_SIZE, dtype=str, keep_default_na=False):
            # Map: локальные счётчики внутри чанка.
            local_skills, local_pairs, local_triples = Counter(), Counter(), Counter()
            for value in chunk["skills_json"]:
                skills = json.loads(value)
                local_skills.update(skills)
                local_pairs.update(itertools.combinations(skills, 2))
                local_triples.update(itertools.combinations(skills, 3))
            # Reduce: объединение счётчиков чанков.
            skill_counts.update(local_skills)
            pair_counts.update(local_pairs)
            triple_counts.update(local_triples)
    finally:
        body.close()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    paths = [
        save_counter(skill_counts, ["skill"], "mapreduce_skills.csv"),
        save_counter(pair_counts, ["skill_1", "skill_2"], "mapreduce_pairs.csv"),
        save_counter(triple_counts, ["skill_1", "skill_2", "skill_3"], "mapreduce_triplets.csv"),
    ]
    for path in paths:
        upload_file(path, f"results/{path.name}")
    elapsed = time.perf_counter() - started
    rss_delta = max(0, process.memory_info().rss - rss_before) / (1024 * 1024)
    return {"method": "MapReduce (S3 streaming + chunks)", "seconds": round(elapsed, 4),
            "python_peak_memory_mb": round(peak / (1024 * 1024), 3),
            "rss_delta_mb": round(rss_delta, 3), "unique_skills": len(skill_counts),
            "unique_pairs": len(pair_counts), "unique_triplets": len(triple_counts)}


def sql_statements(text: str) -> list[str]:
    # В файле запросы разделены точкой с запятой; комментарии сохраняются вместе с запросом.
    return [part.strip() for part in text.split(";") if part.strip() and re.search(r"\bSELECT\b", part, re.I)]


def run_duckdb_from_rustfs(object_key: str = PROCESSED_KEY) -> dict:
    """DuckDB читает тот же обработанный объект через S3-совместимый endpoint RustFS."""
    started = time.perf_counter()
    process = psutil.Process()
    rss_before = process.memory_info().rss
    access = os.environ["RUSTFS_ACCESS_KEY"].replace("'", "''")
    secret = os.environ["RUSTFS_SECRET_KEY"].replace("'", "''")
    region = os.getenv("S3_REGION", "us-east-1").replace("'", "''")
    endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9000").replace("http://", "").replace("https://", "").rstrip("/")
    use_ssl = str(os.getenv("S3_ENDPOINT", "http://localhost:9000")).startswith("https://")
    connection = duckdb.connect()
    try:
        connection.execute("SET threads TO 2")
        connection.execute("SET memory_limit = '2GB'")
        connection.execute("INSTALL httpfs")
        connection.execute("LOAD httpfs")
        connection.execute(
            f"CREATE SECRET rustfs_secret (TYPE S3, KEY_ID '{access}', SECRET '{secret}', "
            f"REGION '{region}', ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL {str(use_ssl).lower()})"
        )
        uri = object_uri(object_key).replace("'", "''")
        connection.execute(f"CREATE VIEW vacancies AS SELECT * FROM read_csv_auto('{uri}', all_varchar=true)")
        statements = sql_statements(SQL_FILE.read_text(encoding="utf-8"))
        outputs = ["duckdb_skills.csv", "duckdb_pairs.csv", "duckdb_triplets.csv"]
        if len(statements) != 3:
            raise RuntimeError(f"Ожидалось 3 SQL-запроса, найдено: {len(statements)}")
        for statement, filename in zip(statements, outputs):
            path = RESULTS / filename
            connection.execute(statement).df().to_csv(path, index=False, encoding="utf-8")
            upload_file(path, f"results/{filename}")
    finally:
        connection.close()
    elapsed = time.perf_counter() - started
    rss_delta = max(0, process.memory_info().rss - rss_before) / (1024 * 1024)
    return {"method": "DuckDB SQL (S3 object scan)", "seconds": round(elapsed, 4), "rss_delta_mb": round(rss_delta, 3)}


def salary_analysis(input_file: Path) -> dict:
    """Медианные зарплаты для пар и троек навыков; учитываются только RUB/RUR."""
    started = time.perf_counter()
    groups = {2: defaultdict(list), 3: defaultdict(list)}
    salaries = []
    for chunk in pd.read_csv(input_file, chunksize=CHUNK_SIZE, dtype=str, keep_default_na=False):
        for _, row in chunk.iterrows():
            raw_salary = row.get("salary_mid", "")
            try:
                salary = float(raw_salary) if raw_salary else None
            except ValueError:
                salary = None
            if salary is None:
                continue
            skills = json.loads(row["skills_json"])
            if not skills:
                continue
            salaries.append(salary)
            for size in (2, 3):
                for combo in itertools.combinations(skills, size):
                    groups[size][combo].append(salary)
    if not salaries:
        print("Зарплатный анализ пропущен: нет зарплат в RUB/RUR.")
        return {"available": False, "seconds": round(time.perf_counter() - started, 4)}
    baseline = statistics.median(salaries)
    for size, combinations in groups.items():
        rows = []
        for combo, values in combinations.items():
            if len(values) < MIN_COMBO_COUNT:
                continue
            record = {f"skill_{i + 1}": skill for i, skill in enumerate(combo)}
            median = statistics.median(values)
            record.update(vacancy_count=len(values), median_salary=median, delta_vs_base=median - baseline)
            rows.append(record)
        columns = [f"skill_{i + 1}" for i in range(size)] + ["vacancy_count", "median_salary", "delta_vs_base"]
        result = pd.DataFrame(rows, columns=columns)
        for label, ascending in (("bottom", True), ("top", False)):
            path = RESULTS / f"salary_{label}_{size}.csv"
            result.sort_values("delta_vs_base", ascending=ascending).to_csv(path, index=False)
            if not result.empty:
                upload_file(path, f"results/{path.name}")
    print(f"Базовая медианная зарплата: {baseline:.2f} RUB; время: {time.perf_counter() - started:.2f} с")
    return {"available": True, "baseline_median_rub": baseline, "salary_rows": len(salaries),
            "min_combo_count": MIN_COMBO_COUNT, "seconds": round(time.perf_counter() - started, 4)}


def regional_analysis(input_file: Path) -> dict:
    """Сводка по трём регионам с наибольшим числом PHP-вакансий."""
    started = time.perf_counter()
    region_counts, salary_values, region_skills = Counter(), defaultdict(list), defaultdict(Counter)
    for chunk in pd.read_csv(input_file, chunksize=CHUNK_SIZE, dtype=str, keep_default_na=False):
        for _, row in chunk.iterrows():
            area = str(row.get("area", "")).strip() or "Не указан"
            region_counts[area] += 1
            try:
                salary = float(row["salary_mid"]) if row.get("salary_mid") else None
            except ValueError:
                salary = None
            if salary is not None:
                salary_values[area].append(salary)
            for skill in set(json.loads(row["skills_json"])):
                region_skills[area][skill] += 1
    top_regions = [area for area, _ in region_counts.most_common(3)]
    summary = []
    skill_rows = []
    for area in top_regions:
        salaries = salary_values[area]
        summary.append({"area": area, "vacancy_count": region_counts[area],
                        "median_salary": statistics.median(salaries) if salaries else None,
                        "vacancies_with_salary": len(salaries)})
        for skill, frequency in region_skills[area].most_common():
            skill_rows.append({"area": area, "skill": skill, "frequency": frequency})
    summary_path, skills_path = RESULTS / "regional_summary.csv", RESULTS / "regional_skills.csv"
    pd.DataFrame(summary, columns=["area", "vacancy_count", "median_salary", "vacancies_with_salary"]).to_csv(summary_path, index=False)
    pd.DataFrame(skill_rows, columns=["area", "skill", "frequency"]).to_csv(skills_path, index=False)
    upload_file(summary_path, "results/regional_summary.csv")
    upload_file(skills_path, "results/regional_skills.csv")
    print("Региональная сводка:\n" + pd.DataFrame(summary).to_string(index=False))
    return {"top_regions": summary, "seconds": round(time.perf_counter() - started, 4)}


def main() -> None:
    if not DATA_FILE.is_file():
        raise FileNotFoundError(f"Положите исходный CSV в {DATA_FILE}. Обязательные столбцы: vacancy_id, name, role, area, key_skills.")
    print("1. Проверка RustFS и загрузка исходного датасета")
    upload_file(DATA_FILE, RAW_KEY)
    print("\n2. Подготовка данных: чтение исходного объекта из RustFS чанками")
    processed_file, data_stats = prepare_data_from_rustfs(RAW_KEY)
    print("\n3. MapReduce: прямое потоковое чтение обработанного объекта из RustFS")
    mapreduce_stats = run_mapreduce_from_rustfs(PROCESSED_KEY)
    print(json.dumps(mapreduce_stats, ensure_ascii=False, indent=2))
    print("\n4. DuckDB: SQL по тому же объекту в RustFS")
    duckdb_stats = run_duckdb_from_rustfs(PROCESSED_KEY)
    print(json.dumps(duckdb_stats, ensure_ascii=False, indent=2))
    print("\n5. Проверка результатов")
    # Импорт по имени файла запускается из корня проекта через uv run src/verification.py.
    from verification import verify_all
    verify_all()
    print("\n6. Зарплатный анализ")
    salary_stats = salary_analysis(processed_file)
    print("\n7. Сравнение регионов")
    regional_stats = regional_analysis(processed_file)
    profile = {"dataset": data_stats, "mapreduce": mapreduce_stats, "duckdb": duckdb_stats,
               "salary": salary_stats, "regions": regional_stats, "chunk_size": CHUNK_SIZE,
               "source_object": f"s3://{bucket_name()}/{RAW_KEY}",
               "processed_object": f"s3://{bucket_name()}/{PROCESSED_KEY}"}
    profile_path = RESULTS / "profile.json"
    profile_path.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
    upload_file(profile_path, "results/profile.json")
    print("\nГотово. Результаты находятся в results/ и в RustFS в префиксе results/.")


if __name__ == "__main__":
    main()
