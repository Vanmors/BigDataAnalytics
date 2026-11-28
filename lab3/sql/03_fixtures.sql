BEGIN;

INSERT INTO anchor.country VALUES
    ('AM', 'Армения'), ('GE', 'Грузия'), ('TR', 'Турция'), ('AE', 'ОАЭ'), ('EG', 'Египет'), ('RS', 'Сербия');
INSERT INTO anchor.airline VALUES ('N', 'Север'), ('M', 'Меридиан');
INSERT INTO anchor.flight_class VALUES ('E', 'Economy'), ('P', 'Premium Economy'), ('B', 'Business');
INSERT INTO anchor.visa_type VALUES ('T', 'Туристическая'), ('M', 'Многократная');
INSERT INTO anchor.visa_status VALUES ('issued', 'Выдана'), ('pending', 'На рассмотрении'), ('revoked', 'Отозвана');
INSERT INTO anchor.booking_status VALUES
    ('booked', 'Создано'), ('confirmed', 'Подтверждено'), ('completed', 'Завершено'), ('cancelled', 'Отменено');

-- Anchor the synthetic calendar once, when PostgreSQL initializes the volume.
CREATE TEMP TABLE fixture_days AS
SELECT n, current_date + (ARRAY[-330, -135, 0])[n] AS load_date FROM generate_series(1, 3) n;

INSERT INTO stg.customer
SELECT load_date, id,
       (ARRAY['Анна Лебедева','Борис Левин','Вера Маркова','Глеб Волков','Дарья Белова','Егор Данилов',
              'Жанна Соколова','Илья Морозов','Кира Попова','Лев Орлов','Мария Ким','Никита Романов'])[id]
           || CASE WHEN id = 2 AND n >= 2 THEN ' (Белов)' ELSE '' END,
       format('DEMO-%s-%s', lpad(id::text, 2, '0'), CASE WHEN id = 2 THEN n ELSE 1 END)
FROM fixture_days CROSS JOIN generate_series(1, 12) id;

INSERT INTO stg.hotel
SELECT load_date, id,
       (ARRAY['Кедр','Маяк','Арка','Атлас','Лазурь','Панорама','Ривьера','Терраса','Оазис','Квартал','Причал','Пальма'])[id]
           || CASE WHEN id = 4 AND n >= 2 THEN ' Плюс' ELSE '' END,
       format('Учебная улица, %s', id + CASE WHEN id = 4 AND n >= 2 THEN 20 ELSE 0 END),
       3 + (id + CASE WHEN id = 4 THEN n ELSE 0 END) % 3,
       4500 + id * 350 + (n - 1) * (400 + id * 25)
FROM fixture_days CROSS JOIN generate_series(1, 12) id;

INSERT INTO stg.flight
SELECT load_date, id, 'DE' || (100 + id + CASE WHEN id = 1 THEN 100 * (n - 1) ELSE 0 END),
       current_date + (ARRAY[-284,-252,-222,-193,-161,-129,-99,-67,6,11,18,25])[id]
           + CASE WHEN id IN (1, 10) THEN n - 1 ELSE 0 END,
       13000 + id * 700 + (n - 1) * (250 + id * 20),
       (ARRAY['E','P','B'])[1 + (id + CASE WHEN id = 1 THEN n ELSE 0 END) % 3],
       (id + CASE WHEN id = 1 THEN n ELSE 0 END)::text || 'A',
       (ARRAY['N','M'])[1 + (id + CASE WHEN id = 1 THEN n ELSE 0 END) % 2]
FROM fixture_days CROSS JOIN generate_series(1, 12) id;

INSERT INTO stg.tour
SELECT load_date, id, trip_day, trip_day + nights,
       (ARRAY['AM','GE','TR','AE','EG','RS'])[CASE WHEN id = 12 AND n >= 2 THEN 1 ELSE 1 + (id - 1) % 6 END],
       CASE WHEN id = 1 THEN n ELSE id END,
       trip_day, trip_day + nights,
       CASE WHEN id = 2 AND n >= 2 THEN 3 ELSE id END
FROM fixture_days CROSS JOIN generate_series(1, 12) id
CROSS JOIN LATERAL (
    SELECT current_date + (ARRAY[-284,-252,-222,-193,-161,-129,-99,-67,6,11,18,25])[id]
               + CASE WHEN id IN (1, 10) THEN n - 1 ELSE 0 END AS trip_day,
           3 + id % 6 + CASE WHEN id = 1 THEN n - 1 ELSE 0 END AS nights
) dates;

INSERT INTO stg.visa
SELECT load_date, id, (ARRAY['AM','GE','TR','AE','EG','RS'])[CASE WHEN id = 12 AND n >= 2 THEN 1 ELSE 1 + (id - 1) % 6 END],
       CASE WHEN (id + CASE WHEN id = 10 THEN n ELSE 0 END) % 2 = 0 THEN 'T' ELSE 'M' END,
       current_date - 380 + (n - 1) * CASE WHEN id = 10 THEN 55 ELSE 0 END,
       current_date + CASE id WHEN 9 THEN -3 WHEN 10 THEN -10 + (n - 1) * 35
                             WHEN 11 THEN -2 WHEN 12 THEN 80 ELSE 80 END,
       CASE WHEN id = 10 AND n = 1 THEN 'pending' ELSE 'issued' END
FROM fixture_days CROSS JOIN generate_series(1, 12) id;

INSERT INTO stg.booking
SELECT load_date, id,
       CASE WHEN id <= 8 AND n = 3 THEN 'completed'
            WHEN id = 15 THEN 'cancelled' WHEN n >= 2 THEN 'confirmed' ELSE 'booked' END,
       32000 + id * 810 + (n - 1) * 470,
       CASE WHEN id = 5 AND n >= 2 THEN 6 WHEN id > 12 THEN 9 + (id - 13) % 4 ELSE id END,
       CASE WHEN id = 3 AND n >= 2 THEN 4 WHEN id > 12 THEN 9 + (id - 13) % 4 ELSE id END,
       CASE WHEN id = 5 AND n >= 2 THEN 6 WHEN id > 12 THEN 9 + (id - 13) % 4 ELSE id END
FROM fixture_days CROSS JOIN LATERAL generate_series(1, (ARRAY[12, 14, 16])[n]) id;

DROP TABLE fixture_days;

COMMIT;
