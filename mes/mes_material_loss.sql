SELECT mn.[MANUFACTURING_OD] AS [工單號],
     mn.[MATERIAL] AS [料號],
     mn.[MATERIAL_DESC] AS [料號名稱],
     CAST(ISNULL(mn.[QTY],0) AS DECIMAL(18,2)) AS [工單預計生產數量],
     mn.[PLANNED_S_DATE] AS [工單預計開工日],
     mn.[PLANNED_C_DATE] AS [工單預計完工日],
     CAST(ISNULL(mn.[QTY_DONE],0) AS DECIMAL(18,2)) AS [工單實際完工數量],
     CAST(ISNULL(mn.[QTY_DONE],0)-ISNULL(mn.[QTY],0) AS DECIMAL(18,2)) AS [實際完工量-預計生產量],
     mn.[ACTUAL_S_DATE] AS [工單實際開工日],
     mn.[ACTUAL_C_DATE] AS [工單實際完工日],
     CASE WHEN mn.[SHOP_ORD_STATUS]=1 THEN '已核' 
            WHEN mn.[SHOP_ORD_STATUS]=2 THEN '生管已派' 
            WHEN mn.[SHOP_ORD_STATUS]=3 THEN '加工中' 
            WHEN mn.[SHOP_ORD_STATUS]=9 THEN '完成' 
            WHEN mn.[SHOP_ORD_STATUS]=10 THEN 'SAP已核' 
            WHEN mn.[SHOP_ORD_STATUS]=12 THEN '已結' 
            WHEN mn.[SHOP_ORD_STATUS]=13 THEN '製造已派' 
            ELSE '' 
       END AS [MES工單狀態],
     CASE WHEN mn.[SAP_ORD_STATUS]='P' THEN '已計畫' 
            WHEN mn.[SAP_ORD_STATUS]='R' THEN '已核發' 
            WHEN mn.[SAP_ORD_STATUS]='C' THEN '已取消' 
            WHEN mn.[SAP_ORD_STATUS]='L' THEN '已結' 
            ELSE '' 
       END AS [SAP工單狀態],
     ISNULL(op.SAP_OPERATION_GP,'') AS [SAP工站代號],
     CASE WHEN op.[SAP_OPERATION_GP]='201' THEN '攪料' 
            WHEN op.[SAP_OPERATION_GP]='202' THEN '碳線' 
            WHEN op.[SAP_OPERATION_GP]='203' THEN '打摺' 
            WHEN op.[SAP_OPERATION_GP]='204' THEN '貼邊' 
            WHEN op.[SAP_OPERATION_GP]='205' THEN '組框' 
            WHEN op.[SAP_OPERATION_GP]='206' THEN '組裝' 
            WHEN op.[SAP_OPERATION_GP]='207' THEN '包裝' 
            WHEN op.[SAP_OPERATION_GP]='101' THEN '填充' 
            WHEN op.[SAP_OPERATION_GP]='104' THEN '包裝' 
            ELSE '' 
       END AS [SAP工站名稱],
     ISNULL(bc.COMPONENT,'') AS [SAP BOM領用料號],
     CAST(ISNULL(bc.QTY,0) AS DECIMAL(18,2)) AS [SAP BOM標準用量],
     CAST(0 AS DECIMAL(18,2)) AS [SAP BOM實際領用量],
     CAST(0 AS DECIMAL(18,2)) AS [SAP工單實際領用量-SAP BOM標準用量],
     CAST(0 AS DECIMAL(18,2)) AS [SAP損耗率],
     ISNULL(bc.ASSY_OP,'') AS [MES工站代號],
     ISNULL(op.OPERATION,'') AS [MES工站名稱],
     CAST(ISNULL(fm.TOTAL_IN_QTY,0) AS DECIMAL(18,2)) AS [MES工單領用數量],
     CASE WHEN bc.QTY IS NULL OR fm.TOTAL_IN_QTY IS NULL OR bc.QTY=0 THEN CAST(0 AS DECIMAL(18,2)) 
            ELSE CAST((ISNULL(fm.TOTAL_IN_QTY,0)-ISNULL(bc.QTY,0)) AS DECIMAL(18,2)) 
       END AS [MES工單領用數量-SAP BOM標準數量],
     CASE WHEN bc.QTY IS NULL OR fm.TOTAL_IN_QTY IS NULL OR bc.QTY=0 OR bc.QTY<0.001 THEN CAST(0 AS DECIMAL(18,2)) 
            ELSE CAST(((ISNULL(fm.TOTAL_IN_QTY,0)-ISNULL(bc.QTY,0))*100.0)/NULLIF(bc.QTY,0) AS DECIMAL(18,2)) 
       END AS [損耗率% (MES實際-SAP標準)]
FROM [yesiang-MES-AP_New].[dbo].[MANUFACTURING_NO] mn
     LEFT JOIN (
    SELECT PLANT, MANUFACTURING_OD, MATERIAL, OPERATION, SUM(ISNULL(IN_QTY,0)) AS TOTAL_IN_QTY
     FROM FEED_MATERIAL_DEVICE
     GROUP BY PLANT,MANUFACTURING_OD,MATERIAL,OPERATION
) fm ON mn.[MANUFACTURING_OD]=fm.[MANUFACTURING_OD]
     LEFT JOIN [BOM_COMPONENT] bc ON mn.[MANUFACTURING_OD]=bc.[BOM] AND mn.[PLANT]=bc.[PLANT] AND fm.[MATERIAL]=bc.[COMPONENT]
     LEFT JOIN [OPERATION] op ON bc.ASSY_OP=op.operation
WHERE mn.[MATERIAL] IS NOT NULL