"""Формирование расширенной сводки для отчёта по результатам анализа."""

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"


def read_csv(filename):
    path = RESULTS / filename

    if not path.exists():
        print(f"Файл не найден: {filename}")
        return None

    return pd.read_csv(path)


def print_table(title, filename, columns, limit=10, ascending=False):
    print(f"\n{title}")
    print("-" * 80)

    df = read_csv(filename)

    if df is None or df.empty:
        print("Нет данных для отображения.")
        return

    available = [column for column in columns if column in df.columns]

    if not available:
        print("Не найдены ожидаемые столбцы.")
        return

    if "frequency" in df.columns:
        df = df.sort_values("frequency", ascending=ascending)

    elif "delta_vs_base" in df.columns:
        df = df.sort_values("delta_vs_base", ascending=ascending)

    print(df[available].head(limit).to_string(index=False))


def main():
    report = []

    def show(text=""):
        print(text)
        report.append(str(text))

    profile_path = RESULTS / "profile.json"

    if profile_path.exists():
        profile = json.loads(
            profile_path.read_text(encoding="utf-8")
        )
    else:
        profile = {}

    # 1. Датасет

    show("=" * 80)
    show("ИТОГОВАЯ СВОДКА ДЛЯ ОТЧЁТА")
    show("=" * 80)

    dataset = profile.get("dataset", {})

    show("\n1. ДАТАСЕТ И ПОДГОТОВКА")
    show(f"Исходных строк: {dataset.get('total_rows', 'нет данных')}")
    show(
        "PHP-вакансий: "
        f"{dataset.get('php_vacancies', 'нет данных')}"
    )
    show(
        "Время подготовки: "
        f"{dataset.get('preparation_seconds', 'нет данных')} с"
    )

    # 2. Навыки

    print_table(
        "2. ТОП-10 ОТДЕЛЬНЫХ НАВЫКОВ",
        "mapreduce_skills.csv",
        ["skill", "frequency"],
    )

    # 3. Пары

    print_table(
        "3. ТОП-10 ПАР НАВЫКОВ",
        "mapreduce_pairs.csv",
        ["skill_1", "skill_2", "frequency"],
    )

    # 4. Тройки

    print_table(
        "4. ТОП-10 ТРОЕК НАВЫКОВ",
        "mapreduce_triplets.csv",
        ["skill_1", "skill_2", "skill_3", "frequency"],
    )

    # 5. Зарплаты

    show("\n5. ЗАРПЛАТНЫЙ АНАЛИЗ")

    salary = profile.get("salary", {})

    baseline = salary.get("baseline_median_rub")

    if baseline is not None:
        show(f"Базовая медианная зарплата: {baseline:,.2f} RUB")
        show(
            "Вакансий с учтённой зарплатой: "
            f"{salary.get('salary_rows', 'нет данных')}"
        )
        show(
            "Минимальное число вакансий для комбинации: "
            f"{salary.get('min_combo_count', 'нет данных')}"
        )

    for size in (2, 3):
        for direction, title in (
            ("top", "наибольшая медиана относительно базы"),
            ("bottom", "наименьшая медиана относительно базы"),
        ):
            filename = f"salary_{direction}_{size}.csv"

            print_table(
                f"{title}: комбинации из {size} навыков",
                filename,
                [
                    f"skill_{i}"
                    for i in range(1, size + 1)
                ] + [
                    "vacancy_count",
                    "median_salary",
                    "delta_vs_base",
                ],
                limit=10,
                ascending=(direction == "bottom"),
            )

    # 6. Регионы

    show("\n6. СРАВНЕНИЕ РЕГИОНОВ")

    regions = read_csv("regional_summary.csv")

    if regions is not None:
        show(regions.to_string(index=False))

        regional_skills = read_csv("regional_skills.csv")

        if regional_skills is not None:
            for area in regions["area"].astype(str):
                show(f"\nСамые востребованные навыки: {area}")

                subset = regional_skills[
                    regional_skills["area"].astype(str) == area
                ]

                subset = subset.sort_values(
                    "frequency",
                    ascending=False,
                )

                show(
                    subset[["skill", "frequency"]]
                    .head(10)
                    .to_string(index=False)
                )

    # 7. Производительность

    show("\n7. ПРОИЗВОДИТЕЛЬНОСТЬ")

    mr = profile.get("mapreduce", {})
    duck = profile.get("duckdb", {})

    if mr:
        show(
            f"MapReduce: {mr.get('seconds')} с; "
            f"пиковая память Python: "
            f"{mr.get('python_peak_memory_mb')} MB; "
            f"изменение RSS: {mr.get('rss_delta_mb')} MB"
        )

    if duck:
        show(
            f"DuckDB: {duck.get('seconds')} с; "
            f"изменение RSS: {duck.get('rss_delta_mb')} MB"
        )

    if mr.get("seconds") and duck.get("seconds"):
        mr_time = float(mr["seconds"])
        duck_time = float(duck["seconds"])

        if mr_time > 0 and duck_time > 0:
            ratio = duck_time / mr_time

            if ratio > 1:
                show(
                    f"По общему измеренному времени MapReduce "
                    f"выполнился примерно в {ratio:.2f} раза быстрее."
                )
            else:
                show(
                    f"По общему измеренному времени DuckDB "
                    f"выполнился примерно в {1 / ratio:.2f} раза быстрее."
                )

            show(
                "Примечание: время DuckDB включает настройку соединения, "
                "httpfs, выполнение SQL и запись результатов."
            )

    show(
        "RSS delta — разница использования памяти до и после функции, "
        "а не пиковое потребление памяти DuckDB."
    )

    # 8. Проверка артефактов

    show("\n8. ПРОВЕРКА СОХРАНЁННЫХ ФАЙЛОВ")

    filenames = [
        "php_vacancies.csv",
        "mapreduce_skills.csv",
        "mapreduce_pairs.csv",
        "mapreduce_triplets.csv",
        "duckdb_skills.csv",
        "duckdb_pairs.csv",
        "duckdb_triplets.csv",
        "salary_top_2.csv",
        "salary_bottom_2.csv",
        "salary_top_3.csv",
        "salary_bottom_3.csv",
        "regional_summary.csv",
        "regional_skills.csv",
        "profile.json",
    ]

    for filename in filenames:
        path = RESULTS / filename

        if path.exists():
            show(
                f"OK: {filename} "
                f"({path.stat().st_size:,} байт)"
                .replace(",", " ")
            )
        else:
            show(f"НЕТ: {filename}")

    # Сохранение отчётной сводки

    output = RESULTS / "report_summary.txt"
    output.write_text(
        "\n".join(report) + "\n",
        encoding="utf-8",
    )

    print(f"\nСводка сохранена в: {output}")


if __name__ == "__main__":
    main()