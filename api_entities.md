Список полей, которые необходимы для витрины cdm.dm_courier_ledger:

Источник - Поле - описание


API GET /couriers - courier_id — ID курьера, которому перечисляем.
API GET /couriers - courier_name — Ф. И. О. курьера.
dds.dm_timestamps - settlement_year — год отчёта.
dds.dm_timestamps -settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
dds.fct_product_sales - orders_count — количество заказов за период (месяц).
dds.fct_product_sales - orders_total_sum — общая стоимость заказов.
API GET / deliveries - rate_avg — средний рейтинг курьера по оценкам пользователей.
dds.fct_product_sales - order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
API GET / deliveries - courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
API GET / deliveries - courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
courier_reward_sum - сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

	