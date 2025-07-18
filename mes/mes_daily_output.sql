SELECT 
    AA.[工單號],
    AA.[料號],
    AA.[料號名稱],
    AA.[工單類型],
    AA.[類型子分類],
    AA.[工單預計生產數量],
    AA.[工單未完工數],
    AA.[工單預計開工日],
    AA.[工單預計完工日],
    AA.[工單實際開工日],
    AA.[工單實際完工日],
    AA.[SAP工單狀態],
    AA.[MES工單狀態],
    AA.[SAP 工站代號],
    AA.[SAP 工站名稱],
    AA.[MES工站代號],
    AA.[MES工站名稱],
    AA.[MES工藝序號],
    AA.[MES工作站狀態],
    AA.[上站良品數量],
    AA.[上站不良品數量],
    AA.[當站良品數量],
    AA.[當站不良品數量],
    AA.[工單實際完工數量],
    AA.[良品批號數量],
    AA.[不良品批號數量]
FROM (
    SELECT　 
        mn.[MANUFACTURING_OD] [工單號], 
        mn.[MATERIAL] [料號],
        mn.[MATERIAL_DESC] [料號名稱],
        CASE 
            WHEN mn.[MO_TYPE] = 10 THEN '一般'
            WHEN mn.[MO_TYPE] = 11 THEN '特殊'
            WHEN mn.[MO_TYPE] = 12 THEN '拆卸'       
        END [工單類型],
        CASE 
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
        END [類型子分類],
        mn.[QTY] [工單預計生產數量], 
        mn.[QTY] - [QTY_DONE] [工單未完工數],
        mn.[PLANNED_S_DATE] [工單預計開工日], 
        mn.[PLANNED_C_DATE] [工單預計完工日], 
        CONVERT(varchar(20), mn.[ACTUAL_S_DATE], 20) [工單實際開工日],
        CONVERT(varchar(20), mn.[ACTUAL_C_DATE], 20) [工單實際完工日], 
        DATEDIFF(dd, mn.[PLANNED_S_DATE], mn.[ACTUAL_S_DATE]) [實際開工日-預計開工日], 
        DATEDIFF(dd, mn.[PLANNED_C_DATE], mn.[ACTUAL_C_DATE]) [實際完工日-預計完工日], 
        CASE
            WHEN mn.[SAP_ORD_STATUS] = 'P' THEN '已計畫'
            WHEN mn.[SAP_ORD_STATUS] = 'R' THEN '已核發'
            WHEN mn.[SAP_ORD_STATUS] = 'C' THEN '已取消'
            WHEN mn.[SAP_ORD_STATUS] = 'L' THEN '已結'
        END [SAP工單狀態],
        CASE 
            WHEN mn.[SHOP_ORD_STATUS] = 1 THEN '已核'
            WHEN mn.[SHOP_ORD_STATUS] = 2 THEN '生管已派'
            WHEN mn.[SHOP_ORD_STATUS] = 3 THEN '加工中'
            WHEN mn.[SHOP_ORD_STATUS] = 9 THEN '完成'
            WHEN mn.[SHOP_ORD_STATUS] = 10 THEN 'SAP已核'
            WHEN mn.[SHOP_ORD_STATUS] = 12 THEN '已結'
            WHEN mn.[SHOP_ORD_STATUS] = 13 THEN '製造已派'
        END [MES工單狀態],
        rs.[STEP_ID] [MES工藝序號],
        CASE  
            WHEN rcds.[DONE] = 1 THEN '已完成' 
            WHEN rcds.[DONE] = 0 THEN '加工中'
        END [MES工作站狀態],
        rs.[OPERATION] [MES工站代號],
        op.[DESCRIPTION] [MES工站名稱],
        op.SAP_OPERATION_GP [SAP 工站代號],
        CASE    
            WHEN op.[SAP_OPERATION_GP] = '101' THEN '填充'
            WHEN op.[SAP_OPERATION_GP] = '102' THEN '中站'
            WHEN op.[SAP_OPERATION_GP] = '103' THEN '組裝'
            WHEN op.[SAP_OPERATION_GP] = '104' THEN '包裝'
            WHEN op.[SAP_OPERATION_GP] = '105' THEN '布套組裝'
            WHEN op.[SAP_OPERATION_GP] = '111' THEN '塑膠射出'
            WHEN op.[SAP_OPERATION_GP] = '112' THEN '上膠'
            WHEN op.[SAP_OPERATION_GP] = '113' THEN '車縫'
            WHEN op.[SAP_OPERATION_GP] = '114' THEN '裁切'
            WHEN op.[SAP_OPERATION_GP] = '201' THEN '攪料'    
            WHEN op.[SAP_OPERATION_GP] = '202' THEN '碳線'    
            WHEN op.[SAP_OPERATION_GP] = '203' THEN '打摺'    
            WHEN op.[SAP_OPERATION_GP] = '204' THEN '貼邊'    
            WHEN op.[SAP_OPERATION_GP] = '205' THEN '組框'
            WHEN op.[SAP_OPERATION_GP] = '206' THEN '組裝'
            WHEN op.[SAP_OPERATION_GP] = '207' THEN '上膠'
            WHEN op.[SAP_OPERATION_GP] = '208' THEN '包裝'
        END AS [SAP 工站名稱],
        (SELECT [QTY_COMPLETED] FROM [RCD_STEP] 
         WHERE [RUN_CARD] = mn.[MANUFACTURING_OD] AND [ROUTER] = rs.[ROUTER] 
         AND [REVISION] = rs.[REVISION] AND [PLANT] = mn.[PLANT]
         AND [STEP_ID] = (SELECT MAX([STEP_ID]) FROM [RCD_STEP] 
                          WHERE [RUN_CARD] = mn.[MANUFACTURING_OD] AND [ROUTER] = rs.[ROUTER] 
                          AND [REVISION] = rs.[REVISION] AND [PLANT] = mn.[PLANT] 
                          AND [STEP_ID] < rs.[STEP_ID])) [上站良品數量],
        (SELECT SUM([QTY]) FROM [PRD_NG_DATA] 
         WHERE [RUN_CARD] = mn.[MANUFACTURING_OD] AND [ROUTER] = rs.[ROUTER]
         AND [STEP_ID] = (SELECT MAX([STEP_ID]) FROM [RCD_STEP] 
                          WHERE [RUN_CARD] = mn.[MANUFACTURING_OD] AND [ROUTER] = rs.[ROUTER] 
                          AND [REVISION] = rs.[REVISION] AND [STEP_ID] < rs.[STEP_ID])) [上站不良品數量],
        rcds.[QTY_COMPLETED] [當站良品數量],
        (SELECT SUM([QTY]) FROM [PRD_NG_DATA] 
         WHERE [RUN_CARD] = mn.[MANUFACTURING_OD] AND [ROUTER] = rs.[ROUTER]
         AND [STEP_ID] = rs.[STEP_ID]) [當站不良品數量],
        mn.[QTY_DONE] [工單實際完工數量],
        (SELECT SUM([IN_QTY]) FROM [DC_BATCHNO_INFO] 
         WHERE [RUN_CARD] = mn.[MANUFACTURING_OD] AND [STEP_ID] = rs.[STEP_ID] 
         AND [STATUS] = 1) [良品批號數量],
        (SELECT SUM([IN_QTY]) FROM [DC_BATCHNO_INFO] 
         WHERE [RUN_CARD] = mn.[MANUFACTURING_OD] AND [STEP_ID] = rs.[STEP_ID] 
         AND [STATUS] = 2) [不良品批號數量]
    FROM [yesiang-MES-AP_New].[dbo].[MANUFACTURING_NO] mn
    LEFT JOIN [yesiang-MES-AP_New].[dbo].[ROUTER_STEP] rs
        ON mn.[MANUFACTURING_OD] = rs.[ROUTER]
    LEFT JOIN [yesiang-MES-AP_New].[dbo].[OPERATION] op
        ON rs.[OPERATION] = op.[OPERATION]
    LEFT JOIN [yesiang-MES-AP_New].[dbo].[RCD_STEP] rcds
        ON rcds.[RUN_CARD] = mn.[MANUFACTURING_OD] 
        AND rcds.[PLANT] = mn.[PLANT] 
        AND rcds.[ROUTER] = rs.[ROUTER]
        AND rcds.[REVISION] = rs.[REVISION] 
        AND rcds.[STEP_ID] = rs.[STEP_ID]
    WHERE mn.MATERIAL IS NOT NULL
) AA