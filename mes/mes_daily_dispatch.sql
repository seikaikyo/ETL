Select	
    mn.[MANUFACTURING_OD] [工單號] 	
   ,mn.[MATERIAL] [料號]	
   ,mn.[MATERIAL_DESC] [料號名稱]	
   ,CASE 	
       WHEN mn.[MO_TYPE] = 10 THEN '一般'	
       WHEN mn.[MO_TYPE] = 11 THEN '特殊'	
       WHEN mn.[MO_TYPE] = 12 THEN '拆卸'       	
    END [工單類型]	
   ,CASE 
       WHEN mn.[TYPE_SUB_CLASS] = 1 THEN '計畫生產'	
       WHEN mn.[TYPE_SUB_CLASS] = 2 THEN '接單生產'	
       WHEN mn.[TYPE_SUB_CLASS] = 3 THEN '試樣生產'	
       WHEN mn.[TYPE_SUB_CLASS] = 4 THEN '組裝工單'
       WHEN mn.[TYPE_SUB_CLASS] = 5 THEN '維修工單'
       WHEN mn.[TYPE_SUB_CLASS] = 6 THEN '拆卸工單'
       WHEN mn.[TYPE_SUB_CLASS] = 7 THEN '委外工單'
       WHEN mn.[TYPE_SUB_CLASS] = 8 THEN '計畫包裝'
       WHEN mn.[TYPE_SUB_CLASS] = 9 THEN '計畫再生'
       WHEN mn.[TYPE_SUB_CLASS] = 10 THEN '重工工單'
       WHEN mn.[TYPE_SUB_CLASS] = 11 THEN '試做生產'
       WHEN mn.[TYPE_SUB_CLASS] = 12 THEN '試產生產'
       WHEN mn.[TYPE_SUB_CLASS] = 13 THEN '卸料工單'
       WHEN mn.[TYPE_SUB_CLASS] = 14 THEN '整修工單'
    END [類型子分類]	
   ,mn.[QTY] [工單預計生產數量] 	
   ,mn.[QTY] - [QTY_DONE] [工單未完工數]	
   ,mn.[PLANNED_S_DATE] [工單預計開工日] 	
   ,mn.[PLANNED_C_DATE] [工單預計完工日] 	
   ,CONVERT(varchar(20), mn.[ACTUAL_S_DATE], 20) [工單實際開工日]	
   ,CONVERT(varchar(20), mn.[ACTUAL_C_DATE], 20) [工單實際完工日] 	
   ,amh.[OPERATION_DESC] [工藝]	
   ,amh.[DEVICE_DESC] [設備]	
   ,amh.[OP_NAME] [作業人員]	
   ,amh.[OP_COUNT] [現場指人數]	
   ,amh.[QTY_M] [現場派工數量]	
   ,CONVERT(varchar(20), amh.[UPDATE_DD], 20) [現場指派時間]	
   ,amh.[ASSIGN_OP] [現場指派人]	
   ,asp.[USER_CNT] [生管指派人數]	
   ,asp.[QTY_M] [生管派工數量]	
   ,CONVERT(varchar(20), asp.[UPDATE_DD], 20) [生管指派時間]	
   ,asp.[ASSIGN_OP] [生管指派人]	
FROM [yesiang-MES-AP_New].[dbo].[MANUFACTURING_NO] mn 		
LEFT JOIN (	
    SELECT 
        a.[MANUFACTURING_OD],
        a.[STEP_ID],
        a.[OPERATION_DESC],
        a.[DEVICE_DESC],
        COUNT(a.USER_NAME) OP_COUNT,
        STRING_AGG(a.USER_NAME, ',') OP_NAME,
        QTY_M,
        b.USER_NAME ASSIGN_OP,
        MAX([UPDATE_DD]) UPDATE_DD
    FROM [yesiang-MES-AP_New].[dbo].[ASSIGN_M_H] a, [yesiang-MES-AP_New].[dbo].[USER] b
    WHERE a.[CREATED_U_ID] = b.[USER_ID]
    GROUP BY a.[MANUFACTURING_OD], a.[STEP_ID], a.[OPERATION_DESC], a.[DEVICE_DESC], a.[QTY_M], b.[USER_NAME]	
) amh ON mn.[MANUFACTURING_OD] = amh.[MANUFACTURING_OD]	
LEFT JOIN (	
    SELECT 
        a.[MANUFACTURING_OD],
        a.[STEP_ID],
        a.[OPERATION_DESC],
        a.[DEVICE_DESC],
        a.[QTY_M],
        a.[USER_CNT],
        b.[USER_NAME] ASSIGN_OP,
        UPDATE_DD
    FROM [yesiang-MES-AP_New].[dbo].[ASSIGN_PLAN] a, [yesiang-MES-AP_New].[dbo].[USER] b
    WHERE a.[CREATED_U_ID] = b.[USER_ID]
) asp ON amh.[MANUFACTURING_OD] = asp.[MANUFACTURING_OD] AND amh.[STEP_ID] = asp.[STEP_ID]
WHERE mn.MATERIAL IS NOT NULL
ORDER BY mn.[MANUFACTURING_OD], amh.[STEP_ID]