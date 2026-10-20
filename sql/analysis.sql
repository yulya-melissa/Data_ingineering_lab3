-- Частоты считаются по уникальным навыкам внутри каждой вакансии.
SELECT skill, COUNT(*) AS frequency
FROM (
    SELECT DISTINCT vacancy_id,
           UNNEST(FROM_JSON(skills_json, '["VARCHAR"]')) AS skill
    FROM vacancies
)
GROUP BY skill
ORDER BY frequency DESC, skill;

-- Порядок пары нормализован по регистронезависимой сортировке,
-- затем по исходному написанию при равенстве casefold.
WITH vacancy_skills AS (
    SELECT DISTINCT vacancy_id,
           UNNEST(FROM_JSON(skills_json, '["VARCHAR"]')) AS skill
    FROM vacancies
)
SELECT a.skill AS skill_1, b.skill AS skill_2, COUNT(*) AS frequency
FROM vacancy_skills a
JOIN vacancy_skills b
  ON a.vacancy_id = b.vacancy_id
 AND (LOWER(a.skill) < LOWER(b.skill)
      OR (LOWER(a.skill) = LOWER(b.skill) AND a.skill < b.skill))
GROUP BY a.skill, b.skill
ORDER BY frequency DESC, skill_1, skill_2;

-- Аналогично нормализуем порядок тройки.
WITH vacancy_skills AS (
    SELECT DISTINCT vacancy_id,
           UNNEST(FROM_JSON(skills_json, '["VARCHAR"]')) AS skill
    FROM vacancies
)
SELECT a.skill AS skill_1, b.skill AS skill_2, c.skill AS skill_3,
       COUNT(*) AS frequency
FROM vacancy_skills a
JOIN vacancy_skills b
  ON a.vacancy_id = b.vacancy_id
 AND (LOWER(a.skill) < LOWER(b.skill)
      OR (LOWER(a.skill) = LOWER(b.skill) AND a.skill < b.skill))
JOIN vacancy_skills c
  ON a.vacancy_id = c.vacancy_id
 AND (LOWER(b.skill) < LOWER(c.skill)
      OR (LOWER(b.skill) = LOWER(c.skill) AND b.skill < c.skill))
GROUP BY a.skill, b.skill, c.skill
ORDER BY frequency DESC, skill_1, skill_2, skill_3;
