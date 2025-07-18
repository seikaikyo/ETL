SELECT
    t0.StartDate 開始日期,
    CASE t0.type
        WHEN 'S' THEN '標準'
        WHEN 'P' THEN '特殊'
        WHEN 'D' THEN '拆卸'
    END 工單類型,
    t0.docnum 工單號碼,
    t0.itemcode 產品號碼,
    t0.prodname 產品名稱,
    CASE t0.status
        WHEN 'R' THEN '已核發'
        WHEN 'L' THEN '已結'
        WHEN 'C' THEN '已取消'
    END 工單狀態,
    t0.plannedqty 計畫數量,
    ISNULL((SELECT SUM(quantity)
    FROM ign1
    WHERE ign1.BaseRef = t0.docnum AND ign1.itemcode = t0.itemcode),0) 實際收貨數量,
    t0.Uom 收貨計量單位,
    t6.Code 路徑階段,
    t5.Name 路徑說明,
    t1.itemcode 發貨號碼,
    t1.ItemName 發貨名稱,
    t1.plannedqty 計畫發貨數量,
    ISNULL((SELECT SUM(quantity)
    FROM ige1
    WHERE ige1.baseref = t0.docnum AND ige1.itemcode = t1.itemcode),0) 實際發貨數量,
    CASE t1.ItemType
        WHEN '4' THEN t7.InvntryUom
        WHEN '290' THEN t8.UnitOfMsr
    END 發貨計量單位
FROM owor t0
    LEFT JOIN wor1 t1 ON t0.docentry = t1.docentry
    LEFT JOIN ign1 t2 ON t0.docentry = t2.baseref
    LEFT JOIN ige1 t3 ON t0.docentry = t3.BaseRef AND t1.ItemCode = t3.itemcode
    LEFT JOIN itt1 t4 ON t0.itemcode = t4.Father AND t1.ItemCode = t4.Code
    LEFT JOIN itt2 t5 ON t0.itemcode = t5.Father AND t1.StageId = t5.StageId
    LEFT JOIN orst t6 ON t5.StgEntry = t6.AbsEntry
    LEFT JOIN oitm t7 ON t1.ItemCode = t7.ItemCode
    LEFT JOIN orsc t8 ON t1.itemcode = t8.VisResCode
WHERE t0.StartDate >= DATEADD(month, -3, GETDATE()) --最近三個月的工單
    AND t0.status IN ('R','L','C') --已核發、已結、已取消
    AND t1.ItemType IN ('4','290')
GROUP BY 
    t0.type, t0.StartDate, t0.docnum, t0.itemcode, t0.prodname, t0.plannedqty, t0.status, 
    t1.PlannedQty, t0.Uom, t1.itemcode, t1.ItemName, t6.Code, t5.name, t1.ItemType, 
    t7.InvntryUom, t8.UnitOfMsr
ORDER BY t0.StartDate