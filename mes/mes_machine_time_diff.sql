WITH 		
HdataList AS (		
    SELECT 		
        mh.GROUP_ID,		
        mh.PLANT,		
        mh.MANUFACTURING_OD,		
        mh.RUN_CARD,		
       COALESCE(o.SAP_OPERATION_GP, mh.OPERATION) AS SAP_OPERATION_GP,		
        mh.STEP_ID,		
        mh.OPERATION,		
        mh.OPERATION_DESC,		
        mh.DEVICE,		
        d.DESCRIPTION DEVICE_DESC,		
        mh.AT_QTY,		
  		
        mh.[COMPONENT] USER_ID,		
        mh.[COMPONENT_DESC] USER_NAME,		
        mh.AT_SUB,		
        mh.COMPONENT_TYPE
    FROM [ACTUAL_WORK_HOUR_MH_D] mh		
    INNER JOIN OPERATION o		
        ON mh.PLANT = o.PLANT AND mh.OPERATION = o.OPERATION		
    INNER JOIN [DEVICE] d		
        ON mh.DEVICE = d.DEVICE 		
    WHERE ISNULL(mh.STATUS, 0) != 9999		
),		
DList_Human AS (		
    SELECT 		
        PLANT,		
        MANUFACTURING_OD,		
        SAP_OPERATION_GP,		
        SUM(CAST(CASE 		
          WHEN ISNUMERIC(COALESCE(AT_SUB, '0')) = 1 		
          THEN AT_SUB 		
          ELSE '0' 		
        END AS DECIMAL(18, 2))) AS Total_Human_Time		
    FROM HdataList WHERE COMPONENT_TYPE = 'HUMAN'		
    GROUP BY PLANT, MANUFACTURING_OD, SAP_OPERATION_GP		
),		
DList_Machine AS (		
    SELECT 		
        PLANT,		
        MANUFACTURING_OD,		
        SAP_OPERATION_GP,		
        SUM(CAST(COALESCE(AT_SUB, '0') AS DECIMAL(18, 2))) AS Total_Machine_Time		
    FROM HdataList WHERE COMPONENT_TYPE = 'MACHINE'		
    GROUP BY PLANT, MANUFACTURING_OD, SAP_OPERATION_GP		
),	
BOM_SAP_STD_MACHINE AS (		
    SELECT 		
        bsl.MANUFACTURING_OD,		
        bsl.PLANT,		
        bsl.ERP_PATH AS SAP_OPERATION_GP,		
        bsl.ERP_PATHNAME,  -- 取得工站名稱		
        SUM(bsl.QTY) AS Standard_Machine_Time  		
    FROM BOM_SAP_LOG bsl		
    WHERE bsl.MATERIAL LIKE 'M%'  -- 只取機台工時		
    GROUP BY bsl.MANUFACTURING_OD, bsl.PLANT, bsl.ERP_PATH, bsl.ERP_PATHNAME		
),		
BOM_SAP_STD_HUMAN AS (		
    SELECT 		
        bsl.MANUFACTURING_OD,		
        bsl.PLANT,		
        bsl.ERP_PATH AS SAP_OPERATION_GP,		
        bsl.ERP_PATHNAME,  -- 取得工站名稱		
        SUM(bsl.QTY) AS Standard_Human_Time  		
    FROM BOM_SAP_LOG bsl		
    WHERE bsl.MATERIAL LIKE 'H%'  -- 只取標準人時		
    GROUP BY bsl.MANUFACTURING_OD, bsl.PLANT, bsl.ERP_PATH, bsl.ERP_PATHNAME		
)		
SELECT 		
    mn.[MANUFACTURING_OD] AS [工單號],		
    mn.[MATERIAL] AS [料號],		
    mn.[MATERIAL_DESC] AS [料號名稱],		
    mn.[QTY] AS [工單預計生產數量],		
    mn.[PLANNED_S_DATE] AS [工單預計開工日],		
    mn.[PLANNED_C_DATE] AS [工單預計完工日],		
    mn.[QTY_DONE] AS [工單實際完工數量],		
    mn.[QTY_DONE] - mn.[QTY] AS [實際完工量-預計生產量],		
    mn.[ACTUAL_S_DATE] AS [工單實際開工日],		
    mn.[ACTUAL_C_DATE] AS [工單實際完工日],		
    CASE 		
        WHEN mn.[SHOP_ORD_STATUS] = 1 THEN '已核'		
        WHEN mn.[SHOP_ORD_STATUS] = 2 THEN '生管已派'		
        WHEN mn.[SHOP_ORD_STATUS] = 3 THEN '加工中'		
        WHEN mn.[SHOP_ORD_STATUS] = 9 THEN '完成'		
        WHEN mn.[SHOP_ORD_STATUS] = 10 THEN 'SAP已核'		
        WHEN mn.[SHOP_ORD_STATUS] = 12 THEN '已結'		
        WHEN mn.[SHOP_ORD_STATUS] = 13 THEN '製造已派'		
    END AS [MES工單狀態],		
    CASE		
        WHEN mn.[SAP_ORD_STATUS] = 'P' THEN '已計畫'		
        WHEN mn.[SAP_ORD_STATUS] = 'R' THEN '已核發'		
        WHEN mn.[SAP_ORD_STATUS] = 'C' THEN '已取消'		
        WHEN mn.[SAP_ORD_STATUS] = 'L' THEN '已結'		
    END AS [SAP工單狀態],		
    COALESCE(bsh.ERP_PATHNAME, bsm.ERP_PATHNAME, '') AS [SAP工站],		
    COALESCE(bsh.Standard_Human_Time, 0) AS [SAP標準人工時],		
    COALESCE(h.Total_Human_Time, 0) AS [MES實際人工時],		
    COALESCE(bsm.Standard_Machine_Time, 0) AS [SAP標準機時],		
    COALESCE(m.Total_Machine_Time, 0) AS [MES實際機時],		
    COALESCE(h.Total_Human_Time, 0) - COALESCE(bsh.Standard_Human_Time, 0) AS [人工時差異(實際-標準)],		
    CASE 		
        WHEN COALESCE(bsh.Standard_Human_Time, 0) = 0 THEN NULL 		
        ELSE 		
            (COALESCE(h.Total_Human_Time, 0) - COALESCE(bsh.Standard_Human_Time, 0)) 		
            / COALESCE(bsh.Standard_Human_Time, 0) * 100 		
    END AS [人時差異比例 % (實際-標準)],		
    COALESCE(m.Total_Machine_Time, 0) - COALESCE(bsm.Standard_Machine_Time, 0) AS [機工時差異(實際-標準)],		
    CASE 		
        WHEN COALESCE(bsm.Standard_Machine_Time, 0) = 0 THEN NULL 		
        ELSE 		
            (COALESCE(m.Total_Machine_Time, 0) - COALESCE(bsm.Standard_Machine_Time, 0)) 		
            / COALESCE(bsm.Standard_Machine_Time, 0) * 100 		
    END AS [機工時差異比例 % (實際-標準)]		
FROM MANUFACTURING_NO mn		
LEFT JOIN DList_Human h 		
    ON mn.PLANT = h.PLANT 		
    AND mn.MANUFACTURING_OD = h.MANUFACTURING_OD		
LEFT JOIN DList_Machine m 		
    ON h.PLANT = m.PLANT 		
    AND h.MANUFACTURING_OD = m.MANUFACTURING_OD		
    AND h.SAP_OPERATION_GP = m.SAP_OPERATION_GP		
LEFT JOIN BOM_SAP_STD_MACHINE bsm 		
    ON h.PLANT = bsm.PLANT		
    AND h.MANUFACTURING_OD = bsm.MANUFACTURING_OD		
    AND h.SAP_OPERATION_GP = bsm.SAP_OPERATION_GP  		
LEFT JOIN BOM_SAP_STD_HUMAN bsh  		
    ON h.PLANT = bsh.PLANT		
    AND h.MANUFACTURING_OD = bsh.MANUFACTURING_OD		
    AND h.SAP_OPERATION_GP = bsh.SAP_OPERATION_GP 		
WHERE mn.MATERIAL IS NOT NULL		
ORDER BY mn.MANUFACTURING_OD, h.SAP_OPERATION_GP