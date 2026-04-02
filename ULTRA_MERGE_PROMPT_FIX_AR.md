# برومبت إصلاح ورفع دقة الدمج داخل نظام QTO

أنت تعمل داخل مشروع QTO خاص بفلل الإمارات. المطلوب ليس مجرد دمج نتائج، بل بناء **Conflict-Resolution Engine** يمنع استبدال الكمية الصحيحة بكمية أكبر أو أضعف أو مشتقة أو متوسطات تاريخية.

## الهدف
رفع دقة الدمج بين:
- نتائج المحرك الرئيسي
- نتائج STR / ARCH / FINISH
- النتائج المشتقة من المعادلات
- النتائج المشتقة من الغرف / الأدلة المكانية
- النتائج المبنية على المتوسطات أو fallback
- بنود external works

## ممنوعات صريحة
1. ممنوع قاعدة: الأكبر يكسب.
2. ممنوع الدمج بالاسم فقط دون item_code و unit و measurement_method.
3. ممنوع أن يطيح بند derived ببند extracted مباشر.
4. ممنوع أن يطيح بند average/fallback ببند extracted أو evidence-derived.
5. ممنوع prefix matching المفتوح إذا كان قد يسبب خلط بنود متشابهة لفظيًا ومختلفة هندسيًا.
6. ممنوع توجيه Layer مجهول إلى كل التخصصات؛ يجب أن يذهب إلى unclassified أو review queue.

## ترتيب الثقة الإلزامي
1. measured-from-geometry
2. schedule/label-confirmed
3. evidence-equation derived from confirmed geometry
4. baseline relation derived
5. learned overlay / sanity clamp
6. engine avg / historical fallback
7. catalog fill

## قواعد الحسم
- extracted > derived > average_scaled > catalog_fill
- exact total > fragmented partial item إذا كانا لنفس البند
- عند اختلاف الوحدات لا يتم الدمج تلقائيًا
- عند اختلاف كبير في الكمية (>25%) لا يتم الاستبدال إلا إذا كانت رتبة الثقة أعلى بوضوح
- إذا كانت الكمية الجديدة sparse fragment أصغر من 35% من كمية قوية موجودة، يتم رفض الاستبدال
- البنود المعتمدة على مساحة الجدران والتشطيبات تحتاج حماية إضافية من sparse overrides

## المطلوب من الكود
1. إنشاء semantic merge key مركب من:
   - item_code
   - discipline
   - unit
   - measurement_method
   - zone/floor إن وجد
2. إنشاء resolver يحسم التعارض حسب:
   - source rank
   - quality rank
   - similarity ratio
   - unit compatibility
   - explicit total vs fragment
3. حفظ provenance واضح لكل بند نهائي:
   - winner source
   - rejected candidates
   - reason for selection
4. إدخال manual review queue عند التعارضات الكبيرة بدل الدمج الإجباري

## مخرجات مقبولة فقط
- لا يوجد override أعمى بسبب كمية أكبر
- لا يوجد derived يطيح extracted مباشر
- لا يوجد avg fallback يطيح measured quantity
- كل بند نهائي له سبب اختيار واضح
- الكود modular وقابل للاختبار

## اختبارات قبول إلزامية
1. extracted=420 م² و derived=395 م² لنفس البند → الفائز extracted
2. average=390 م² و sparse extracted fragment=8.4 م² → الفائز average/strong value وليس fragment
3. catalog fill=0 و derived=55 → الفائز derived
4. exact total=120 و fragmented sum candidates=113 → الفائز exact total
5. unit mismatch بين m² و lm → لا merge تلقائي
6. extracted zero و derived positive موثوق → derived يملأ الفراغ

## أسلوب التنفيذ
- عدّل أقل عدد ممكن من الملفات
- لا تكسر الواجهات الحالية
- أضف helper module مستقل للدمج
- اجعل القرار evidence-first وليس quantity-first
