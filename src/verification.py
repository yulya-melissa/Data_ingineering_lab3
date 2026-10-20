"""Сравнение результатов собственного MapReduce и DuckDB."""
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"


def load_counts(filename: str, columns: list[str]) -> dict:
    path = RESULTS / filename
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    counts = {}
    for _, row in df.iterrows():
        key = tuple(row[column] for column in columns)
        counts[key] = int(float(row["frequency"]))
    return counts


def compare_results(name: str, map_file: str, sql_file: str, columns: list[str]) -> bool:
    left, right = load_counts(map_file, columns), load_counts(sql_file, columns)
    differences = [(key, left.get(key), right.get(key)) for key in left.keys() | right.keys()
                   if left.get(key) != right.get(key)]
    if differences:
        print(f"ОШИБКА: {name}; расхождений: {len(differences)}")
        for key, a, b in sorted(differences, key=lambda item: str(item[0]))[:20]:
            print(f"{key}: MapReduce={a}, DuckDB={b}")
        return False
    print(f"OK: {name}; совпало комбинаций: {len(left)}")
    return True


def verify_all() -> bool:
    checks = [
        ("Отдельные навыки", "mapreduce_skills.csv", "duckdb_skills.csv", ["skill"]),
        ("Пары навыков", "mapreduce_pairs.csv", "duckdb_pairs.csv", ["skill_1", "skill_2"]),
        ("Тройки навыков", "mapreduce_triplets.csv", "duckdb_triplets.csv", ["skill_1", "skill_2", "skill_3"]),
    ]
    missing = [file for _, a, b, _ in checks for file in (a, b) if not (RESULTS / file).is_file()]
    if missing:
        raise FileNotFoundError("Не найдены файлы результатов: " + ", ".join(sorted(set(missing))))
    results = [compare_results(*check) for check in checks]
    if not all(results):
        raise SystemExit(1)
    print("Верификация пройдена: результаты MapReduce и DuckDB совпадают.")
    return True


if __name__ == "__main__":
    verify_all()
