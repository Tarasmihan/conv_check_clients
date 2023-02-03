SELECT
  /* Транзакция рейтейл - Одна позиция в чеке, без учета количества.  */
  d_data AS data,
  dictGetString('ops_thes', 'mrc', toUInt64(i_index_id)) as mrc,
  dictGetString('ops_thes', 'ufps', toUInt64(i_index_id)) as ufps,
  dictGetString('ops_thes', 'pochtamt', toUInt64(i_index_id)) as pochtamt,
  if(dictGetString('ops_thes', 'ops_type', toUInt64(i_index_id)) IN ('ГОПС', 'ППС'),'ГОПС','СОПС') as ops_type,
  dictGetString('ops_thes', 'tu_code', toUInt64(i_index_id)) as rss,
  dictGetString('ops_thes', 'sv_code', toUInt64(i_index_id)) as rm,     
  dictGetUInt32('ops_thes', 'index_ops', toUInt64(i_index_id)) as index_ops, 
  pr AS proj,
  toFloat32(t0.summa) AS fact, -- сумма продаж ретейл
  toFloat32(t0.qty) as qty, -- количество проданных товаров ретейл
  toFloat32(t0.retail_transaction) AS retail_transaction, -- количество транзакций по товару ретейл
  toFloat32(t0.retail_check) AS retail_check, -- количество уникальных чеков ретейл
  --toFloat32(t1.test_all_transaction) AS test_all_transaction, -- TEST количество транзакций по всем бизнесам
  toFloat32(t1.all_transaction) AS all_transaction, -- количество транзакций по всем бизнесам
  toFloat32(t1.all_check) AS all_check, --  количество чеков по всем бизнесам
  toFloat32(t2.clientsflow) AS clientsflow, -- количество клиентов (клиентопоток)
  toFloat32(t4.clientsflow_prev_year) as clientsflow_prev_year,
  toFloat32(t3.retail_check_prev_year) as retail_check_prev_year
FROM
  (
  SELECT  
    coalesce(t0.data, t3.data_key) AS d_data,
    coalesce(t0.index_id, t3.index_id) as i_index_id,
    coalesce(t0.proj, t3.proj) as pr,
    *
  FROM
    (
    WITH if(project = '', 'Розница 1.0', project) AS proj 
    SELECT 
      toStartOfMonth(trans_date) AS data,
      index_id,
      proj, 
      SUM(amount) AS summa, 
      SUM(qty) AS qty, 
      COUNT() AS retail_transaction, 
      uniqExact(cheqway, index_id, trans_date) AS retail_check --поскольку нумерация чеков обнуляется, группировка идет в пределах дня
    FROM 
      read.retail_olap 
    WHERE 
      1 = 1 
      AND dictGetString('product_thes', 'prod_group', (dictGetUInt8('ops_thes', 'region', toUInt64(index_id)), item_id)) = 'Товары' 
      AND sales_id = 'Терминал' 
      AND trans_date BETWEEN '2022-01-01' and '2023-01-31'
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
      COUNT() AS all_check 
    FROM 
      read.etf_raw --Витрина предназначена для хранения данных по качеству обслуживания клиентов в ОПС с СУО (транзакции, ср. время чека, ср. сумма чека и тд.)
    WHERE 
      1 = 1 
      AND trans_date BETWEEN '2022-01-01' and '2023-01-31'
    GROUP BY 
      data, 
      index_id
    ) AS t1 ON t0.data = t1.data AND t0.index_id = t1.index_id

    ALL LEFT JOIN (
    SELECT 
      toStartOfMonth(oper_date) AS data,
      index_id,
      SUM(clients_qty) AS clientsflow --clients_qty - кол-во клиентов
    FROM 
      read.clients_flow_olap -- Витрина используется при расчете конверсии - retail check/ clientsflow, т.е. Кол-во розничных чеков, включая РПО, разделить на Общее количество клиентов.
    WHERE 
      1 = 1
      AND oper_date BETWEEN '2022-01-01' and '2023-01-31'
    GROUP BY 
      index_id,
      data
    ) AS t2 ON t0.data = t2.data AND t0.index_id = t2.index_id

    FULL JOIN (
    WITH if(project = '', 'Розница 1.0', project) AS proj
    SELECT 
      toStartOfMonth(trans_date) AS data,
      data + interval 1 year as data_key,
      index_id,
    proj,
      uniqExact(cheqway, index_id, trans_date) AS retail_check_prev_year --поскольку нумерация чеков обнуляется, группировка идет в пределах дня
    FROM 
      read.retail_olap 
    WHERE 
      1 = 1 
      AND dictGetString('product_thes', 'prod_group', (dictGetUInt8('ops_thes', 'region', toUInt64(index_id)), item_id)) = 'Товары' 
      AND sales_id = 'Терминал' 
      AND trans_date BETWEEN '2021-01-01' and '2022-01-31'
    GROUP BY 
      data, 
      index_id,
    proj
    ) AS t3 ON t0.data = t3.data_key AND t0.index_id = t3.index_id

    ALL LEFT JOIN (
    SELECT 
      toStartOfMonth(oper_date) AS data,
      index_id,
      SUM(clients_qty) AS clientsflow_prev_year --clients_qty - кол-во клиентов
    FROM 
      read.clients_flow_olap -- Витрина используется при расчете конверсии - retail check/ clientsflow, т.е. Кол-во розничных чеков, включая РПО, разделить на Общее количество клиентов.
    WHERE 
      1 = 1
      AND oper_date BETWEEN '2021-01-01' and '2022-01-31'
    GROUP BY 
      index_id,
      data
    ) AS t4 ON t3.data = t4.data AND t3.index_id = t4.index_id  
  )
WHERE
  1 = 1
ORDER BY
  data,
  mrc,
  ufps,
  pochtamt,
  ops_type,
  rss,
  rm,
  index_ops
SETTINGS
  join_use_nulls = 1