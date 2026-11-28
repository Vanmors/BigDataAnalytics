\pset pager off
\echo
\echo '1. Текущие профили клиентов'
SELECT * FROM report.customer_profile ORDER BY customer_id;
\echo
\echo '2. Стоимость услуг в день создания бронирования'
SELECT * FROM report.booking_cost ORDER BY created_on, booking_id;
\echo
\echo '3. История цены отеля № 1'
SELECT * FROM report.hotel_price_history WHERE hotel_id = 1 ORDER BY valid_from;
\echo
\echo '4. Визы, истёкшие к вылету'
SELECT DISTINCT full_name, destination, flight_date, expires_on
FROM report.visa_assessment WHERE problem = 'Истекла' ORDER BY flight_date, full_name;
SELECT valid_from, valid_to, expires_on FROM anchor.visa_expiry WHERE id = 10 ORDER BY valid_from;
\echo
\echo '5. Пять отелей с наибольшим числом ночей за последние 12 календарных месяцев'
SELECT hotel_id, hotel_name, sum(nights) AS nights
FROM report.hotel_month CROSS JOIN report.clock
WHERE month BETWEEN date_trunc('month', as_of) - interval '11 months' AND date_trunc('month', as_of)
GROUP BY hotel_id, hotel_name ORDER BY nights DESC, hotel_id LIMIT 5;
\echo
\echo '6. Непрерывная история стоимости тура № 1'
SELECT * FROM report.tour_cost_history(1);
\echo
\echo '7. Средняя стоимость по кварталу вылета'
SELECT extract(quarter FROM flight_date)::integer AS quarter, round(avg(package_cost), 2) AS average_cost
FROM report.booking_cost GROUP BY quarter ORDER BY quarter;
