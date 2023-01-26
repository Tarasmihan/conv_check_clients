SELECT
  /* Транзакция рейтейл - Одна позиция в чеке, без учета количества. */
  t0.data AS data,
  dictGetString('ops_thes', 'mrc', toUInt64(t0.index_id)) as mrc,
  dictGetString('ops_thes', 'ufps', toUInt64(t0.index_id)) as ufps,
  dictGetString('ops_thes', 'pochtamt', toUInt64(t0.index_id)) as pochtamt,
  if(dictGetString('ops_thes', 'ops_type', toUInt64(t0.index_id)) IN ('ГОПС', 'ППС'),'ГОПС','СОПС') as ops_type,
  dictGetString('ops_thes', 'tu_code', toUInt64(t0.index_id)) as rss,
  dictGetString('ops_thes', 'sv_code', toUInt64(t0.index_id)) as rm,     
  dictGetUInt32('ops_thes', 'index_ops', toUInt64(t0.index_id)) as index_ops, 
  t0.proj AS proj,
  toFloat32(t0.summa) AS fact, -- сумма продаж ретейл
  toFloat32(t0.qty) as qty, -- количество проданных товаров ретейл
  toFloat32(t0.retail_transaction) AS retail_transaction, -- количество транзакций по товару ретейл
  toFloat32(t0.retail_chek) AS retail_chek, -- количество уникальных чеков ретейл
  --toFloat32(t1.test_all_transaction) AS test_all_transaction, -- TEST количество транзакций по всем бизнесам
  toFloat32(t1.all_transaction) AS all_transaction, -- количество транзакций по всем бизнесам
  toFloat32(t1.all_chek) AS all_chek, --  количество чеков по всем бизнесам
  toFloat32(t2.clientsflow) AS clientsflow, -- количество клиентов (клиентопоток)
  toFloat32(fact / retail_chek) AS avg_retail_check, -- средний чек ретейл
  toFloat32(fact / retail_transaction) AS avg_retail_transaction_price, -- среднеяя выручка за транзакцию,
  --CASE WHEN data = toStartOfMonth(today() - interval 2 day) THEN fact/toDayOfMonth(today() - interval 2 day) * toDayOfMonth(toLastDayOfMonth(today() - interval 2 day)) ELSE null END AS RR_retail,
  retail_chek / clientsflow as conversion_retail,
  clientsflow / toFloat32(t4.clientsflow_prev_year) - 1 as LFL_clientsflow,
  conversion_retail - toFloat32(t3.retail_chek_prev_year)/ toFloat32(t4.clientsflow_prev_year) as LFL_conversion
FROM
  (
  WITH if(project = '', 'Розница 1.0', project) AS proj 
  SELECT 
    toStartOfMonth(trans_date) AS data,
    data - interval 1 year as data_key,
    index_id,
    proj, 
    SUM(amount) AS summa, 
    SUM(qty) AS qty, 
    COUNT() AS retail_transaction, 
    uniqExact((cheqway, index_id, trans_date)) AS retail_chek --поскольку нумерация чеков обнуляется, группировка идет в пределах дня
  FROM 
    read.retail_olap 
  WHERE 
    1 = 1 
    --AND toStartOfMonth(trans_date) BETWEEN '2021-01-01' AND '2021-12-31' --AND toStartOfMonth(trans_date) BETWEEN toStartOfMonth(today()  - interval 6 month) AND today()
    --AND toStartOfMonth(trans_date) BETWEEN '2023-01-01' AND today()
    --AND toStartOfMonth(trans_date) BETWEEN toStartOfMonth(today()  - interval 6 month) AND today()
    AND dictGetString('product_thes', 'prod_group', (dictGetUInt8('ops_thes', 'region', toUInt64(index_id)), item_id)) = 'Товары' 
    AND sales_id = 'Терминал' 
    AND 
    (
    trans_date BETWEEN toStartOfMonth(today() - interval 2 month) and today() - interval 2 day
    OR trans_date BETWEEN toStartOfMonth(today() - interval 2 month - interval 1 year) and toLastDayOfMonth(today() + interval 2 month - interval 1 year)
    OR trans_date BETWEEN toStartOfMonth(today() - interval 2 month - interval 2 year) and toLastDayOfMonth(today() + interval 2 month - interval 2 year)
    )
  GROUP BY 
    data, 
    index_id, 
    proj
  ) AS t0 
  ALL LEFT JOIN (
  SELECT 
    toStartOfMonth(trans_date) AS data, 
    index_id,
    SUM(arrayUniq(service.name)) as test_all_transaction,
    SUM(trans_cnt) AS all_transaction, --trans_cnt - кол-во "пиков" на кассе
    COUNT() AS all_chek 
  FROM 
    read.etf_raw --Витрина предназначена для хранения данных по качеству обслуживания клиентов в ОПС с СУО (транзакции, ср. время чека, ср. сумма чека и тд.)
  WHERE 
    1 = 1 
    --AND toStartOfMonth(trans_date) BETWEEN '2023-01-01' AND today()
    --AND toStartOfMonth(trans_date) BETWEEN toStartOfMonth(today()  - interval 6 month) AND today()
    AND 
    (
    trans_date BETWEEN toStartOfMonth(today() - interval 2 month) and today() - interval 2 day
    OR trans_date BETWEEN toStartOfMonth(today() - interval 2 month - interval 1 year) and toLastDayOfMonth(today() + interval 2 month - interval 1 year)
    OR trans_date BETWEEN toStartOfMonth(today() - interval 2 month - interval 2 year) and toLastDayOfMonth(today() + interval 2 month - interval 2 year)
    )
  GROUP BY 
    data, 
    index_id
  ) AS t1 ON t0.data = t1.data AND t0.index_id = t1.index_id
  ALL LEFT JOIN (
  SELECT 
    SUM(clients_qty) AS clientsflow, --clients_qty - кол-во клиентов
    index_id,
    toStartOfMonth(oper_date) AS data 
  FROM 
    read.clients_flow_olap -- Витрина используется при расчете конверсии - retail chek/ clientsflow, т.е. Кол-во розничных чеков, включая РПО, разделить на Общее количество клиентов.
  WHERE 
    1 = 1
    --oper_date BETWEEN '2023-01-01' AND today()
    AND
    (
    oper_date BETWEEN toStartOfMonth(today() - interval 2 month) and today() - interval 2 day
    OR oper_date BETWEEN toStartOfMonth(today() - interval 2 month - interval 1 year) and toLastDayOfMonth(today() + interval 2 month - interval 1 year)
    OR oper_date BETWEEN toStartOfMonth(today() - interval 2 month - interval 2 year) and toLastDayOfMonth(today() + interval 2 month - interval 2 year)
    )  
  GROUP BY 
    index_id,
    data
  ) AS t2 ON t0.data = t2.data AND t0.index_id = t2.index_id
  ALL LEFT JOIN (
  SELECT 
    toStartOfMonth(trans_date) AS data,
    index_id,
    uniqExact((cheqway, index_id, trans_date)) AS retail_chek_prev_year --поскольку нумерация чеков обнуляется, группировка идет в пределах дня
  FROM 
    read.retail_olap 
  WHERE 
    1 = 1 
    --AND toStartOfMonth(trans_date) BETWEEN '2021-01-01' AND '2021-12-31' --AND toStartOfMonth(trans_date) BETWEEN toStartOfMonth(today()  - interval 6 month) AND today()
    --AND toStartOfMonth(trans_date) BETWEEN '2023-01-01' AND today()
    --AND toStartOfMonth(trans_date) BETWEEN toStartOfMonth(today()  - interval 6 month) AND today()
    AND dictGetString('product_thes', 'prod_group', (dictGetUInt8('ops_thes', 'region', toUInt64(index_id)), item_id)) = 'Товары' 
    AND sales_id = 'Терминал' 
    AND 
    (
    trans_date BETWEEN toStartOfMonth(today() - interval 2 month) and today() - interval 2 day
    OR trans_date BETWEEN toStartOfMonth(today() - interval 2 month - interval 1 year) and toLastDayOfMonth(today() + interval 2 month - interval 1 year)
    OR trans_date BETWEEN toStartOfMonth(today() - interval 2 month - interval 2 year) and toLastDayOfMonth(today() + interval 2 month - interval 2 year)
    )
  GROUP BY 
    data, 
    index_id
  ) AS t3 ON t0.data_key = t3.data AND t0.index_id = t3.index_id
  ALL LEFT JOIN (
  SELECT 
    SUM(clients_qty) AS clientsflow_prev_year, --clients_qty - кол-во клиентов
    index_id,
    toStartOfMonth(oper_date) AS data 
  FROM 
    read.clients_flow_olap -- Витрина используется при расчете конверсии - retail chek/ clientsflow, т.е. Кол-во розничных чеков, включая РПО, разделить на Общее количество клиентов.
  WHERE 
    1 = 1
    --oper_date BETWEEN '2023-01-01' AND today()
    AND
    (
    oper_date BETWEEN toStartOfMonth(today() - interval 2 month) and today() - interval 2 day
    OR oper_date BETWEEN toStartOfMonth(today() - interval 2 month - interval 1 year) and toLastDayOfMonth(today() + interval 2 month - interval 1 year)
    OR oper_date BETWEEN toStartOfMonth(today() - interval 2 month - interval 2 year) and toLastDayOfMonth(today() + interval 2 month - interval 2 year)
    )  
  GROUP BY 
    index_id,
    data
) AS t4 ON t0.data_key = t4.data AND t0.index_id = t4.index_id  
ORDER BY
  data,
  mrc,
  ufps,
  pochtamt,
  ops_type,
  rss,
  rm,
  index_ops
