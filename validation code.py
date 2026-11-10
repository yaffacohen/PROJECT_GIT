import os
import re
import logging
from datetime import datetime
from google.cloud import bigquery
from collections import namedtuple

# הגדרת הלוגר להצגת התראות ושגיאות בצורה שקופה
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Project and Dataset settings
PROJECT_ID = "ita-phuket-nonprod-data"
DATASET = "QASH"              # דאטהסט ראשי המכיל את כל טבלאות הבדיקה ב-Non-Prod
GOLD_KEYS_DATASET = "gold"    # סכמת ה-Gold החיצונית - משמשת לשאיבת מפתחות לוגיים במקרה שלא הוזן מפתח ידני
client = bigquery.Client(project=PROJECT_ID)

# ==========================================
# הגדרת טבלאות מטרה לבדיקה:
# פורמט: (שם טבלת Gold ב-QASH, שם טבלת On-Prem ב-QASH, מפתח ידני אופציונלי)
# אם המפתח השלישי הוא None או מחרוזת ריקה, הסקריפט ישלוף אותו אוטומטית מה־Gold.
# ==========================================
TARGET_TABLES = [
    ("main_program_tik_mh_shana_details", "vw_tik_mh_shana_details", "tik, shana"),
    ("main_program_bikorot_sfarim", "vw_bikorot_sfarim", "tik, shana"),
    ("main_program_meyda_hashuv_1214", "vw_meyda_hashuv_1214", "tik, shana"),
    ("main_program_meyda_hashuv_1301", "vw_meyda_hashuv_1301", "tik, shana")
]

def run_qash_validation():
    print("\n--- Starting QASH Validation & Deep Reconciliation (Manual/Auto Keys) ---")
    
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    metrics_diff_table = f"`{PROJECT_ID}.{DATASET}.validation_table_metrics_diff_{run_id}`"
    schema_and_stats_table = f"`{PROJECT_ID}.{DATASET}.validation_table_schema_and_stats_{run_id}`"
    reconciliation_table = f"`{PROJECT_ID}.{DATASET}.validation_deep_data_reconciliation_{run_id}`"
    
    print(f"Validation run ID: {run_id}")
    
    # 1. יצירת טבלת מדדי מאקרו (metrics_diff_table)
    client.query(f"""
        CREATE TABLE IF NOT EXISTS {metrics_diff_table} (
          table_name STRING,
          date_key STRING,
          onprem_row_count INT64,
          gold_row_count INT64,
          row_count_diff INT64,
          onprem_max_update_date TIMESTAMP,
          gold_max_update_date TIMESTAMP,
          explanation STRING,
          is_valid STRING,
          validation_timestamp TIMESTAMP
        )
    """).result()
    
    # 2. יצירת טבלת פערי סכמה וסטטיסטיקות נומריות
    client.query(f"""
        CREATE TABLE IF NOT EXISTS {schema_and_stats_table} (
          table_name STRING,
          diff_type STRING,
          column_name STRING,
          onprem_value STRING,
          gold_value STRING,
          gap_reason STRING,
          is_valid STRING,
          validation_timestamp TIMESTAMP
        )
    """).result()
    
    # 3. יצירת טבלת הרקונסילציה העמוקה (רשומה מול רשומה)
    client.query(f"""
        CREATE TABLE IF NOT EXISTS {reconciliation_table} (
          table_name STRING,
          gap_type STRING,
          validation_timestamp TIMESTAMP,
          key_name STRING,
          key_value STRING,
          column_name STRING,
          onprem_value STRING,
          gold_value STRING,
          gap_reason STRING,
          is_valid STRING
        )
    """).result()
    
    # טעינת רשימת הטבלאות לבדיקה
    if TARGET_TABLES:
        TablePair = namedtuple('TablePair', ['base_gold_tbl', 'gold_tbl', 'onprem_tbl', 'clean_name', 'manual_keys'])
        pairs = []
        for item in TARGET_TABLES:
            g_tbl = item[0]
            o_tbl = item[1]
            m_keys = item[2] if len(item) > 2 else None
            base_g = g_tbl[5:] if g_tbl.startswith('gold_') else g_tbl
            pairs.append(TablePair(
                base_gold_tbl=base_g, 
                gold_tbl=g_tbl, 
                onprem_tbl=o_tbl, 
                clean_name=base_g,
                manual_keys=m_keys
            ))
        print(f"Target filter active: Processing {len(pairs)} explicit table pairs.")
    else:
        pairs_query = f"""
            SELECT 
              REGEXP_REPLACE(g.table_name, '^gold_', '') AS base_gold_tbl,
              g.table_name AS gold_tbl,
              o.table_name AS onprem_tbl,
              REGEXP_REPLACE(g.table_name, '^gold_', '') AS clean_name
            FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.TABLES` g
            INNER JOIN `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.TABLES` o
              ON o.table_name = CONCAT('onprem_enriched_', REGEXP_REPLACE(g.table_name, '^gold_', ''))
            WHERE g.table_type IN ('BASE TABLE', 'CLONE')
              AND g.table_name LIKE 'gold_%'
              AND o.table_type IN ('BASE TABLE', 'CLONE')
        """
        raw_pairs = list(client.query(pairs_query).result())
        TablePair = namedtuple('TablePair', ['base_gold_tbl', 'gold_tbl', 'onprem_tbl', 'clean_name', 'manual_keys'])
        pairs = [TablePair(base_gold_tbl=r.base_gold_tbl, gold_tbl=r.gold_tbl, onprem_tbl=r.onprem_tbl, clean_name=r.clean_name, manual_keys=None) for r in raw_pairs]
        print(f"No target filter. Auto-detected {len(pairs)} table pairs in QASH.")
    
    classified_tik_list = [520033200, 500104674, 520027475, 512704867, 558263166, 500100433, 500107875, 512875071, 500104708, 557746567, 540164647, 557652633, 500107909, 500107883, 520043027, 500104799, 558268413, 513417592, 500103528, 500105960, 500100664, 512394701, 587400003, 557663481, 511007700, 520035874, 557636560, 500107172, 512074444, 500107891, 500104831, 558202743, 500104856, 514507052, 558032314, 587400102, 500104724, 557937489, 520040122, 520042185, 520036369, 500107917]
    classified_tik_str = ", ".join([str(t) for t in classified_tik_list])

    for row in pairs:
        base_gold_tbl = row.base_gold_tbl
        gold_tbl = row.gold_tbl
        onprem_tbl = row.onprem_tbl
        clean_name = row.clean_name
        manual_keys = row.manual_keys
        
        print(f"\nProcessing validation for: {clean_name} (Gold: {DATASET}.{gold_tbl}, OnPrem: {DATASET}.{onprem_tbl})")
        
        try:
            def get_upd_expr(tbl, ds_name):
                q = f"""
                  SELECT IF(EXISTS(
                    SELECT 1 FROM `{PROJECT_ID}.{ds_name}.INFORMATION_SCHEMA.COLUMNS` 
                    WHERE table_name = '{tbl}' AND LOWER(column_name) = 'update_date'
                  ), 'SAFE_CAST(MAX(update_date) AS TIMESTAMP)', 'CAST(NULL AS TIMESTAMP)') as expr
                """
                res = list(client.query(q).result())
                return res[0].expr if res else 'CAST(NULL AS TIMESTAMP)'
            
            onprem_upd_expr = get_upd_expr(onprem_tbl, DATASET)
            gold_upd_expr = get_upd_expr(gold_tbl, DATASET)
            
            date_key_q = f"""
              SELECT 
                EXISTS(SELECT 1 FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{onprem_tbl}' AND LOWER(column_name) = 'date_key')
                AND
                EXISTS(SELECT 1 FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{gold_tbl}' AND LOWER(column_name) = 'date_key') as has_date_key
            """
            has_date_key = list(client.query(date_key_q).result())[0].has_date_key

            is_deleted_check_q = f"""
              SELECT EXISTS(
                SELECT 1 FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` 
                WHERE table_name = '{onprem_tbl}' AND LOWER(column_name) = 'is_deleted'
              ) as has_is_deleted
            """
            has_is_deleted = list(client.query(is_deleted_check_q).result())[0].has_is_deleted
            onprem_filter_cond = "COALESCE(CAST(o.is_deleted AS INT64), 0) = 0" if has_is_deleted else "1=1"
            
            tik_field_check_q = f"""
              SELECT column_name 
              FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` 
              WHERE table_name = '{gold_tbl}' AND LOWER(column_name) IN ('tik_id', 'tik_mh', 'tik')
            """
            tik_fields = [r.column_name.lower() for r in client.query(tik_field_check_q).result()]
            
            security_tik_count = 0
            if any(f in tik_fields for f in ['tik_id', 'tik_mh', 'tik']):
                tik_col = 'tik' if 'tik' in tik_fields else ('tik_id' if 'tik_id' in tik_fields else 'tik_mh')
                sec_q = f"""
                    SELECT COUNT(DISTINCT t) as cnt
                    FROM UNNEST([{classified_tik_str}]) t
                    WHERE t IN (SELECT SAFE_CAST({tik_col} AS INT64) FROM `{PROJECT_ID}.{DATASET}.{gold_tbl}`)
                      AND t NOT IN (SELECT SAFE_CAST({tik_col} AS INT64) FROM `{PROJECT_ID}.{DATASET}.{onprem_tbl}` o WHERE ({onprem_filter_cond}))
                """
                try:
                    sec_res = list(client.query(sec_q).result())
                    security_tik_count = sec_res[0].cnt if sec_res else 0
                except Exception as e:
                    logging.warning(f"Failed to calculate security_tik_count for {clean_name}: {e}")
                    security_tik_count = 0

            if security_tik_count > 0:
                explanation_expr = f"""
                  CASE 
                    WHEN (COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0)) = 0 THEN 'תקין - אין פער'
                    WHEN (COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0)) > 0 AND g.max_upd > o.max_upd 
                    THEN CONCAT('הפער מוסבר על ידי שילוב של תיקים ביטחוניים (n=', {security_tik_count}, ') ותאריך עדכון מאוחר יותר בגולד')
                    WHEN (COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0)) > 0 AND (COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0)) <= {security_tik_count} 
                    THEN CONCAT('הפער מכוסה במלואו על ידי תיקים ביטחוניים (נמצאו {security_tik_count} תיקים ביטחוניים)')
                    WHEN (COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0)) > 0 AND g.max_upd > o.max_upd 
                    THEN 'תאריך עדכון מקסימלי בגולד גדול יותר - הפער נובע מהכנסת רשומות חדשות'
                    ELSE CONCAT('פער לא מוסבר של ', CAST((COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0)) AS STRING), ' רשומות ללא הצדקת תאריך עדכון או תיקים ביטחוניים מספקים')
                  END
                """
            else:
                explanation_expr = f"""
                  CASE 
                    WHEN (COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0)) = 0 THEN 'תקין - אין פער'
                    WHEN (COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0)) > 0 AND g.max_upd > o.max_upd 
                    THEN 'תאריך עדכון מקסימלי בגולד גדול יותר - הפער נובע כנראה מהכנסת רשומות חדשות'
                    ELSE CONCAT('פער לא מוסבר של ', CAST((COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0)) AS STRING), ' רשומות ללא תאריך עדכון מעודכן בגולד')
                  END
                """

            is_valid_expr = f"""
              CASE 
                WHEN (COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0)) = 0 THEN 'yes'
                WHEN (COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0)) > 0 
                     AND (
                       ({security_tik_count} > 0 AND (COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0)) <= {security_tik_count})
                       OR g.max_upd > o.max_upd
                     ) 
                THEN 'yes'
                ELSE 'no'
              END
            """

            if has_date_key:
                metrics_ins = f"""
                    INSERT INTO {metrics_diff_table}
                    WITH onprem_data AS (
                      SELECT CAST(SUBSTR(CAST(date_key AS STRING), 1, 4) AS INT64) as yr, COUNT(*) as cnt, {onprem_upd_expr} as max_upd 
                      FROM `{PROJECT_ID}.{DATASET}.{onprem_tbl}` o
                      WHERE SAFE_CAST(date_key AS INT64) >= 2015 AND ({onprem_filter_cond})
                      GROUP BY yr
                    ),
                    gold_data AS (
                      SELECT CAST(SUBSTR(CAST(date_key AS STRING), 1, 4) AS INT64) as yr, COUNT(*) as cnt, {gold_upd_expr} as max_upd 
                      FROM `{PROJECT_ID}.{DATASET}.{gold_tbl}` 
                      WHERE SAFE_CAST(date_key AS INT64) >= 2015
                      GROUP BY yr
                    )
                    SELECT 
                      '{clean_name}' as table_name,
                      CAST(COALESCE(o.yr, g.yr) AS STRING) as date_key,
                      COALESCE(o.cnt, 0) as onprem_row_count,
                      COALESCE(g.cnt, 0) as gold_row_count,
                      COALESCE(g.cnt, 0) - COALESCE(o.cnt, 0) as row_count_diff,
                      o.max_upd as onprem_max_update_date,
                      g.max_upd as gold_max_update_date,
                      {explanation_expr} as explanation,
                      {is_valid_expr} as is_valid,
                      CURRENT_TIMESTAMP() as validation_timestamp
                    FROM onprem_data o
                    FULL OUTER JOIN gold_data g ON o.yr = g.yr
                """
            else:
                metrics_ins = f"""
                    INSERT INTO {metrics_diff_table}
                    WITH onprem_data AS (
                      SELECT COUNT(*) as cnt, {onprem_upd_expr} as max_upd FROM `{PROJECT_ID}.{DATASET}.{onprem_tbl}` o WHERE ({onprem_filter_cond})
                    ),
                    gold_data AS (
                      SELECT COUNT(*) as cnt, {gold_upd_expr} as max_upd FROM `{PROJECT_ID}.{DATASET}.{gold_tbl}`
                    )
                    SELECT 
                      '{clean_name}' as table_name,
                      'ALL' as date_key,
                      o.cnt as onprem_row_count,
                      g.cnt as gold_row_count,
                      g.cnt - o.cnt as row_count_diff,
                      o.max_upd as onprem_max_update_date,
                      g.max_upd as gold_max_update_date,
                      {explanation_expr} as explanation,
                      {is_valid_expr} as is_valid,
                      CURRENT_TIMESTAMP() as validation_timestamp
                    FROM onprem_data o, gold_data g
                """
            client.query(metrics_ins).result()
            
            # 1. MISSING_IN_GOLD (סכמה)
            client.query(f"""
                INSERT INTO {schema_and_stats_table}
                WITH raw_diffs AS (
                  SELECT 
                    LOWER(column_name) as column_name,
                    CASE 
                      WHEN LOWER(column_name) IN ('update_date', 'insert_date', 'zman', 'insert_date_utc', 'update_date_utc', 'rocket_timestamp', 'adabas_timestamp', 'inserttimestamp', 'source_system', 'operation_type', 'first_seen', 'last_seen', 'isn', 'rcrdid')
                        OR LOWER(column_name) LIKE '%zman%' 
                        OR LOWER(column_name) LIKE '%timestamp%' 
                        OR LOWER(column_name) LIKE '%first_seen%'
                        OR LOWER(column_name) LIKE '%last_seen%'
                      THEN 'שדה תפעולי'
                      ELSE 'פער סכמה: עמודה קיימת במקור אך לא בטבלת ה-Gold'
                    END as gap_reason
                  FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
                  WHERE table_name = '{onprem_tbl}'
                    AND LOWER(column_name) NOT IN (
                      SELECT LOWER(column_name) 
                      FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` 
                      WHERE table_name = '{gold_tbl}'
                    )
                )
                SELECT 
                  '{clean_name}' as table_name,
                  'MISSING_IN_GOLD' as diff_type,
                  column_name,
                  'Column exists in On-Prem' as onprem_value,
                  'NULL' as gold_value,
                  gap_reason,
                  IF(gap_reason IS NOT NULL AND gap_reason NOT LIKE 'פער סכמה: עמודה%', 'yes', 'no') as is_valid,
                  CURRENT_TIMESTAMP() as validation_timestamp
                FROM raw_diffs
            """).result()

            # 2. MISSING_IN_ONPREM (סכמה)
            client.query(f"""
                INSERT INTO {schema_and_stats_table}
                WITH raw_diffs AS (
                  SELECT 
                    LOWER(column_name) as column_name,
                    CASE 
                      WHEN LOWER(column_name) LIKE '%original_tik%' 
                        OR LOWER(column_name) LIKE '%tik_original%' 
                        OR LOWER(column_name) LIKE '%tik_name%' 
                      THEN 'העשרה בגולד'
                      ELSE 'פער סכמה: עמודה קיימת ב Gold אך לא בטבלת המקור'
                    END as gap_reason
                  FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
                  WHERE table_name = '{gold_tbl}'
                    AND LOWER(column_name) NOT IN (
                      SELECT LOWER(column_name) 
                      FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` 
                      WHERE table_name = '{onprem_tbl}'
                    )
                )
                SELECT 
                  '{clean_name}' as table_name,
                  'MISSING_IN_ONPREM' as diff_type,
                  column_name,
                  'NULL' as onprem_value,
                  'Column exists in Gold' as gold_value,
                  gap_reason,
                  IF(gap_reason IS NOT NULL AND gap_reason NOT LIKE 'פער סכמה: עמודה%', 'yes', 'no') as is_valid,
                  CURRENT_TIMESTAMP() as validation_timestamp
                FROM raw_diffs
            """).result()
            
            check_diffs_q = f"""
                SELECT LOGICAL_OR(row_count_diff != 0) as has_row_diff
                FROM {metrics_diff_table}
                WHERE table_name = '{clean_name}'
            """
            row_diff_res = list(client.query(check_diffs_q).result())
            has_row_diff = row_diff_res[0].has_row_diff if row_diff_res else False
            schema_diff_check = list(client.query(f"SELECT COUNT(*) as cnt FROM {schema_and_stats_table} WHERE table_name = '{clean_name}'").result())[0].cnt
            
            if not has_row_diff and schema_diff_check == 0:
                print(f" -> No gaps found for {clean_name}. Skipping deep reconciliation.")
                continue

            # ==========================================
            # שליפת המפתח: מתוך TARGET_TABLES או מתיאור הטבלה ב-Gold
            # ==========================================
            v_keys_string = manual_keys
            
            if not v_keys_string:
                keys_q = f"""
                  SELECT option_value 
                  FROM `{PROJECT_ID}.{GOLD_KEYS_DATASET}.INFORMATION_SCHEMA.TABLE_OPTIONS`
                  WHERE table_name = '{base_gold_tbl}' AND option_name = 'description'
                """
                keys_res = list(client.query(keys_q).result())
                if keys_res and keys_res[0].option_value:
                    opt_val = keys_res[0].option_value
                    match = re.search(r'(?i)(?:logical|logic)\s*key:\s*(.+)', opt_val, re.DOTALL)
                    if match:
                        raw_key = match.group(1)
                        cleaned = re.sub(r'[\r\n]+', ' ', raw_key)
                        v_keys_string = re.sub(r'[.,\s\-\"]+$', '', cleaned.strip())
            
            has_valid_keys = False
            if v_keys_string:
                keys_list = [k.strip() for k in v_keys_string.split(',') if k.strip()]
                valid_check = True
                for k in keys_list:
                    k_check_q = f"""
                      SELECT 
                        EXISTS(SELECT 1 FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{onprem_tbl}' AND LOWER(column_name) = LOWER('{k}'))
                        AND
                        EXISTS(SELECT 1 FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{gold_tbl}' AND LOWER(column_name) = LOWER('{k}')) as exists_both
                    """
                    if not list(client.query(k_check_q).result())[0].exists_both:
                        valid_check = False
                        break
                has_valid_keys = valid_check
            
            if has_valid_keys:
                keys_list = [k.strip() for k in v_keys_string.split(',') if k.strip()]
                join_condition = " AND ".join([f"CAST(o.`{k}` AS STRING) = CAST(g.`{k}` AS STRING)" for k in keys_list])
                key_value_expr_onprem = " || ' | ' || ".join([f"COALESCE(CAST(o.`{k}` AS STRING), 'NULL')" for k in keys_list])
                key_value_expr_gold = " || ' | ' || ".join([f"COALESCE(CAST(g.`{k}` AS STRING), 'NULL')" for k in keys_list])
                gold_keys_null = " OR ".join([f"g.`{k}` IS NULL" for k in keys_list])
                onprem_keys_null = " OR ".join([f"o.`{k}` IS NULL" for k in keys_list])
                
                onprem_filter_where = " AND ".join([f"o.`{k}` IS NOT NULL" for k in keys_list])
                gold_filter_where = " AND ".join([f"g.`{k}` IS NOT NULL" for k in keys_list])

                # בניית התנאי המאוחד כולל is_deleted והתאריכים
                active_onprem_filters = [onprem_filter_cond]
                if has_date_key:
                    active_onprem_filters.append("SAFE_CAST(o.date_key AS INT64) >= 2015")
                active_onprem_filters.append(f"({onprem_filter_where})")

                onprem_cte_where = " AND ".join(active_onprem_filters)
                gold_cte_where = f"SAFE_CAST(g.date_key AS INT64) >= 2015 AND ({gold_filter_where})" if has_date_key else f"({gold_filter_where})"

                # רשומות שחסרות בטבלת ה-Gold ב-QASH (מתחשב ב-is_deleted ובשנים)
                client.query(f"""
                    INSERT INTO {reconciliation_table}
                    SELECT 
                      '{clean_name}' as table_name,
                      'MISSING_IN_GOLD' as gap_type,
                      CURRENT_TIMESTAMP() as validation_timestamp,
                      '{v_keys_string}' as key_name,
                      ({key_value_expr_onprem}) as key_value,
                      'N/A' as column_name,
                      'Record exists in On-Prem but missing in Gold' as onprem_value,
                      CAST(NULL AS STRING) as gold_value,
                      CAST(NULL AS STRING) as gap_reason,
                      'no' as is_valid
                    FROM `{PROJECT_ID}.{DATASET}.{onprem_tbl}` o
                    LEFT JOIN `{PROJECT_ID}.{DATASET}.{gold_tbl}` g
                      ON {join_condition}
                    WHERE {onprem_cte_where} AND ({gold_keys_null})
                """).result()

                is_classified_key_gold = " OR ".join([f"CAST(g.`{k}` AS INT64) IN ({classified_tik_str})" for k in keys_list if k.lower() in ('tik_id', 'tik_mh', 'tik')])
                classified_reason_expr_gold = f"CASE WHEN {is_classified_key_gold if is_classified_key_gold else 'FALSE'} THEN 'תיק ביטחוני' ELSE CAST(NULL AS STRING) END"

                # רשומות שחסרות במקור
                client.query(f"""
                    INSERT INTO {reconciliation_table}
                    SELECT 
                      '{clean_name}' as table_name,
                      'MISSING_IN_ONPREM' as gap_type,
                      CURRENT_TIMESTAMP() as validation_timestamp,
                      '{v_keys_string}' as key_name,
                      ({key_value_expr_gold}) as key_value,
                      'N/A' as column_name,
                      CAST(NULL AS STRING) as onprem_value,
                      'Record exists in Gold but missing in On-Prem' as gold_value,
                      COALESCE({classified_reason_expr_gold}, CAST(NULL AS STRING)) as gap_reason,
                      IF({classified_reason_expr_gold} IS NOT NULL, 'yes', 'no') as is_valid
                    FROM `{PROJECT_ID}.{DATASET}.{gold_tbl}` g
                    LEFT JOIN `{PROJECT_ID}.{DATASET}.{onprem_tbl}` o
                      ON {join_condition}
                    WHERE {gold_cte_where} AND ({onprem_keys_null})
                """).result()

                # בדיקות סטטיסטיקות לעמודות נומריות (כעת מסנן is_deleted ושנים)
                num_cols_q = f"""
                  SELECT o.column_name, o.data_type 
                  FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` o
                  INNER JOIN `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` g
                    ON LOWER(o.column_name) = LOWER(g.column_name) AND o.data_type = g.data_type
                  WHERE o.table_name = '{onprem_tbl}' AND g.table_name = '{gold_tbl}'
                    AND (
                      UPPER(o.data_type) LIKE '%INT%' OR 
                      UPPER(o.data_type) LIKE '%FLOAT%' OR 
                      UPPER(o.data_type) LIKE '%NUMERIC%' OR 
                      UPPER(o.data_type) LIKE '%DECIMAL%' OR 
                      UPPER(o.data_type) LIKE '%BIGNUMERIC%'
                    )
                """
                num_col_rows = list(client.query(num_cols_q).result())
                
                if num_col_rows:
                    num_structs = []
                    for n_row in num_col_rows:
                        c = n_row.column_name
                        num_structs.append(f"""
                            STRUCT(
                              '{c}' AS column_name,
                              CAST(SAFE_CAST(MIN(o.`{c}`) AS FLOAT64) AS FLOAT64) AS onprem_min,
                              CAST(SAFE_CAST(MIN(g.`{c}`) AS FLOAT64) AS FLOAT64) AS gold_min,
                              CAST(SAFE_CAST(MAX(o.`{c}`) AS FLOAT64) AS FLOAT64) AS onprem_max,
                              CAST(SAFE_CAST(MAX(g.`{c}`) AS FLOAT64) AS FLOAT64) AS gold_max,
                              CAST(SAFE_CAST(SUM(o.`{c}`) AS FLOAT64) AS FLOAT64) AS onprem_sum,
                              CAST(SAFE_CAST(SUM(g.`{c}`) AS FLOAT64) AS FLOAT64) AS gold_sum,
                              CAST(COUNTIF(o.`{c}` IS NULL) AS INT64) AS onprem_nulls,
                              CAST(COUNTIF(g.`{c}` IS NULL) AS INT64) AS gold_nulls
                            )
                        """)
                    
                    num_structs_list = ", \n".join(num_structs)

                    numeric_stats_ins = f"""
                        INSERT INTO {schema_and_stats_table}
                        WITH onprem_filtered AS (
                          SELECT * FROM `{PROJECT_ID}.{DATASET}.{onprem_tbl}` o WHERE {onprem_cte_where}
                        ),
                        gold_filtered AS (
                          SELECT * FROM `{PROJECT_ID}.{DATASET}.{gold_tbl}` g WHERE {gold_cte_where}
                        ),
                        aggregated_stats AS (
                          SELECT 
                            [
                              {num_structs_list}
                            ] as stats_arr
                          FROM onprem_filtered o
                          INNER JOIN gold_filtered g
                            ON {join_condition}
                        ),
                        flattened_stats AS (
                          SELECT 
                            '{clean_name}' as table_name,
                            s.column_name,
                            s.onprem_min, s.gold_min,
                            s.onprem_max, s.gold_max,
                            s.onprem_sum, s.gold_sum,
                            s.onprem_nulls, s.gold_nulls
                          FROM aggregated_stats,
                          UNNEST(stats_arr) s
                        )
                        SELECT table_name, 'NUMERIC_MIN_MISMATCH', column_name, CAST(onprem_min AS STRING), CAST(gold_min AS STRING), 'פער בערך מינימלי', 'yes', CURRENT_TIMESTAMP() FROM flattened_stats WHERE onprem_min IS DISTINCT FROM gold_min
                        UNION ALL
                        SELECT table_name, 'NUMERIC_MAX_MISMATCH', column_name, CAST(onprem_max AS STRING), CAST(gold_max AS STRING), 'פער בערך מקסימלי', 'yes', CURRENT_TIMESTAMP() FROM flattened_stats WHERE onprem_max IS DISTINCT FROM gold_max
                        UNION ALL
                        SELECT table_name, 'NUMERIC_SUM_MISMATCH', column_name, CAST(onprem_sum AS STRING), CAST(gold_sum AS STRING), 'פער בסכום כולל (Sum)', 'yes', CURRENT_TIMESTAMP() FROM flattened_stats WHERE onprem_sum IS DISTINCT FROM gold_sum
                        UNION ALL
                        SELECT table_name, 'NUMERIC_NULLS_MISMATCH', column_name, CAST(onprem_nulls AS STRING), CAST(gold_nulls AS STRING), 'פער בכמות ערכי NULL', 'yes', CURRENT_TIMESTAMP() FROM flattened_stats WHERE onprem_nulls IS DISTINCT FROM gold_nulls
                    """
                    client.query(numeric_stats_ins).result()

                # רקונסילציית ערכים ברמת השדה (גם כן כעת מסנן is_deleted ושנים)
                cols_q = f"""
                  SELECT o.column_name, o.data_type 
                  FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` o
                  INNER JOIN `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS` g
                    ON LOWER(o.column_name) = LOWER(g.column_name) AND o.data_type = g.data_type
                  WHERE o.table_name = '{onprem_tbl}' AND g.table_name = '{gold_tbl}'
                  ORDER BY o.column_name
                """
                col_rows = list(client.query(cols_q).result())
                
                if col_rows:
                    structs_exprs = []
                    mismatch_exprs = []
                    for row_col in col_rows:
                        c = row_col.column_name
                        dt = row_col.data_type.upper()
                        
                        if 'STRING' in dt:
                            comp_o = f"REGEXP_REPLACE(TRIM(REPLACE(o.`{c}`, '\\u00a0', ' ')), r'[[:space:]]+', ' ')"
                            comp_g = f"REGEXP_REPLACE(TRIM(REPLACE(g.`{c}`, '\\u00a0', ' ')), r'[[:space:]]+', ' ')"
                            val_o  = f"REGEXP_REPLACE(TRIM(REPLACE(CAST(o.`{c}` AS STRING), '\\u00a0', ' ')), r'[[:space:]]+', ' ')"
                            val_g  = f"REGEXP_REPLACE(TRIM(REPLACE(CAST(g.`{c}` AS STRING), '\\u00a0', ' ')), r'[[:space:]]+', ' ')"
                        else:
                            comp_o = f"o.`{c}`"
                            comp_g = f"g.`{c}`"
                            if 'TIMESTAMP' in dt or 'DATETIME' in dt:
                                val_o = f"FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S UTC', CAST(o.`{c}` AS TIMESTAMP), 'UTC')"
                                val_g = f"FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S UTC', CAST(g.`{c}` AS TIMESTAMP), 'UTC')"
                            else:
                                val_o = f"CAST(o.`{c}` AS STRING)"
                                val_g = f"CAST(g.`{c}` AS STRING)"
                        
                        structs_exprs.append(f"STRUCT('{c}' AS column_name, {val_o} AS onprem_value, {val_g} AS gold_value)")
                        mismatch_exprs.append(f"{comp_o} IS DISTINCT FROM {comp_g}")
                    
                    structs_list = ", ".join(structs_exprs)
                    mismatch_where = " OR ".join(mismatch_exprs)
                    key_value_expr = " || ' | ' || ".join([f"COALESCE(CAST(o.`{k}` AS STRING), CAST(g.`{k}` AS STRING))" for k in keys_list])

                    reconciliation_ins = f"""
                        INSERT INTO {reconciliation_table}
                        WITH onprem_filtered AS (
                          SELECT * FROM `{PROJECT_ID}.{DATASET}.{onprem_tbl}` o WHERE {onprem_cte_where}
                        ),
                        gold_filtered AS (
                          SELECT * FROM `{PROJECT_ID}.{DATASET}.{gold_tbl}` g WHERE {gold_cte_where}
                        ),
                        joined_data AS (
                          SELECT 
                            '{clean_name}' as table_name,
                            'VALUE_MISMATCH' as gap_type,
                            CURRENT_TIMESTAMP() as validation_timestamp,
                            ({key_value_expr}) as key_value,
                            [
                              {structs_list}
                            ] as diffs
                          FROM onprem_filtered o
                          INNER JOIN gold_filtered g
                            ON {join_condition}
                          WHERE ({mismatch_where})
                        )
                        SELECT 
                          table_name,
                          gap_type,
                          validation_timestamp,
                          '{v_keys_string}' as key_name,
                          key_value,
                          d.column_name,
                          d.onprem_value,
                          d.gold_value,
                          CASE 
                            WHEN LOWER(d.column_name) IN ('insert_date', 'update_date', 'zman', 'insert_date_utc', 'update_date_utc', 'rocket_timestamp', 'adabas_timestamp', 'inserttimestamp', 'source_system', 'operation_type', 'first_seen', 'last_seen', 'isn', 'rcrdid') 
                                 THEN 'שדה תפעולי - מוחרג מהשוואה'
                            ELSE 'פער לא מוגדר'
                          END as gap_reason,
                          CASE 
                            WHEN (
                                 LOWER(d.column_name) IN ('insert_date', 'update_date', 'zman', 'insert_date_utc', 'update_date_utc', 'rocket_timestamp', 'adabas_timestamp', 'inserttimestamp', 'source_system', 'operation_type', 'first_seen', 'last_seen', 'isn', 'rcrdid')
                            ) THEN 'yes'
                            ELSE 'no'
                          END as is_valid
                        FROM joined_data,
                        UNNEST(diffs) d
                        WHERE (d.onprem_value IS DISTINCT FROM d.gold_value)
                    """
                    client.query(reconciliation_ins).result()

        except Exception as e:
            logging.error(f"Error processing table {clean_name}: {e}")
            print(f" ERROR: Skipped table {clean_name} due to an unexpected error. Moving to next table.")

    print("\nValidation process completed successfully.")

if __name__ == "__main__":
    run_qash_validation()